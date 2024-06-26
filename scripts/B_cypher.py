from neo4j import GraphDatabase
import csv
import os


def write_to_csv(data, filename):
    output_path = '/Users/zzy13/Desktop/Classes_at_UPC/SDM_Semantic_data_management/Lab_1/Codes/Data/query_result'
    file_path = os.path.join(output_path, filename)
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if data:  # Write header only if data is not empty
            writer.writerow([key for key in data[0].keys()])
        for row in data:
            writer.writerow(row.values())


# Query 1: Find the top 3 most cited papers of each conference. 
def find_top3_cited(session):
    query1 = """
    MATCH ()-[c:cites]->(p:Paper)-[:presented_in]->(con:Conference)
    WITH con, p, count(c) as cite_num
    WITH con, p, cite_num ORDER BY cite_num DESC
    RETURN con as conference, collect(p)[0..3] as top_papers, collect(cite_num)[0..3] as cite_numbers
    """
    results1 = session.run(query1)

    data = []
    for record in results1:
        data.append({"Conference": record['conference'], "TopPapers": record['top_papers'], "CitationNumbers":record['cite_numbers']})
    output_path = '/Users/zzy13/Desktop/Classes_at_UPC/SDM_Semantic_data_management/Lab_1/Codes/Data/query_result'
    filename = "top_cited_papers.csv"
    file_path = os.path.join(output_path, filename)
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Conference", "Paper", "CitationNumber"])  # Write header
        for row in data:
            conference_name = row["Conference"]["name"]
            top_papers = row["TopPapers"]
            cite_numbers = row["CitationNumbers"]
            for paper, cite_number in zip(top_papers, cite_numbers):
                paper_title = paper["title"]
                writer.writerow([conference_name, paper_title, cite_number])
    # write_to_csv(data, "top_cited_papers.csv")
    print(f"top_cited_papers.csv written")


# Query 2: For each conference find its community: i.e., those authors that have published papers on that conference in, at least, 4 different editions. 
def find_commu(session):
    query2 = """
    MATCH (p:Paper)-[:written_by]->(a:Authors), (p)-[in_con:presented_in]->(con:Conference),
                (p)-[pub_in:published_in_year]->(y:Year)
    WITH  a, con, count(pub_in) as tot_pubs
    WHERE tot_pubs > 3
    RETURN  a.author_name as author_name, con.name as conference_name, tot_pubs
    """
    results2 = session.run(query2)

    data = []
    for record in results2:
        data.append({"Author": record['author_name'], "Conference": record['conference_name'], "Total Publications": record['tot_pubs']})

    write_to_csv(data, "conference_communities.csv")
    print(f"conference_communities.csv written")

# Find the impact factors of the journals in your graph
# // To calculate the impact factor of a journal, we need to count the number of times the articles published in that journal were cited in a particular year (called Year0). We then look at the number of articles published in the same journal in the previous two years (Year0-1 and Year0-2) and count how many times they were cited in Year0 as well. Once we have these values, we can use them to calculate the impact factor of the journal. However, we can only calculate the impact factor for those journals and years where there were publications and citations in all three years (Year0, Year0-1, and Year0-2). Any combinations where data is missing for any of these years will be automatically disqualified.
def find_if(session):
    """
    This function calculates and retrieves the impact factor for each journal in the graph.
    """
    query3 = """
    MATCH ()-[cite:cites]->(p0:Paper)-[:published_in]->(jnl:Journal)-[:in_year]-(y0:Year)
    MATCH (p0)-[:published_in_year]->(y0)
    WITH jnl, toInteger(y0.year) AS year0, count(cite) as cite_num

    MATCH (p1:Paper)-[:published_in]->(jnl)
    MATCH (p1)-[pub1:published_in_year]->(y1:Year)
    WHERE toInteger(y1.year) = year0 - 1
    WITH jnl, year0, cite_num, count(pub1) AS pub_num1

    MATCH (p2:Paper)-[:published_in]->(jnl)
    MATCH (p2)-[pub2:published_in_year]->(y2:Year)
    WHERE toInteger(y2.year) = year0 - 2
    WITH jnl, year0, cite_num, pub_num1, count(pub2) AS pub_num2

    RETURN jnl.journal_name AS Journal, year0 AS ImpactFactorYear, cite_num/(pub_num1+pub_num2) AS ImpactFactor
    """
    results = session.run(query3)

    data = []
    for record in results:
        data.append({"Journal": record['Journal'], "Impact Factor Year": record['ImpactFactorYear'], "Impact Factor": record['ImpactFactor']})

    write_to_csv(data, "journal_impact_factors.csv")
    print(f"journal_impact_factors.csv written")


# Find the h-indexes of the authors in your graph
def find_hindex(session):
    query = """
    MATCH (author:Authors)<-[:written_by]-(paper:Paper)
    RETURN author, collect(paper) AS papers
    """
    results = session.run(query)

    data = []
    for record in results:
        author = record['author']
        papers = record['papers']

        citation_list = []
        for paper in papers:
            paper_doi = paper['doi']
            result = session.run("""
            MATCH (paperCited:Paper {doi: $paperDoi})<-[:cites]-(paper)
            RETURN count(*) AS citations
            """, paperDoi=paper_doi)
            citation_list.append(result.single()['citations'])

        citation_list.sort(reverse=True)
        h_index = 0
        for citations in citation_list:
            if citations <= h_index:
                break
            h_index += 1

        data.append({"Author": author['author_name'], "h-index": h_index})

    write_to_csv(data, "author_h_indexes.csv")
    print(f"Author_h_indexes.csv written")


def main():
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j+s://"
    AUTH = ("neo4j", "your_password")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print("connection successful!")
        with driver.session(database="neo4j") as session:
                session.execute_write(find_top3_cited)
                session.execute_write(find_commu)
                session.execute_write(find_if)
                session.execute_write(find_hindex)
                print('Results output successfully!')

if __name__ == "__main__":
     main()