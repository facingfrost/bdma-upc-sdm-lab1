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


# Query 1: Find the top 3 most cited papers of each conference. --- DONE 
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
    write_to_csv(data, "top_cited_papers.csv")
    print(f"Top 3 Cited Papers written to top_cited_papers.csv")


########################## Ongoing ##########################
# Query 2: For each conference find its community: i.e., those authors that have published papers on that conference in, at least, 4 different editions.
def find_commu(session):
    query2 = """
    MATCH (a:Authors)-[:CONTRIBUTED]->(p:Paper)-[in_col:presented_in]->(pro:Conference),
                (p)-[pub_in:published_in]->(y:Year)
    WITH  a, pro, count(pub_in) as tot_pubs
    WHERE tot_pubs > 3
    RETURN  a, pro, tot_pubs
    """
    # MATCH (conf:Conference)<-[:presented_in]-(paper:Paper)<-[:written_by]-(author:Authors)
    # WITH conf.name AS conference, author
    # MATCH (author)-[:written_by]->(other:Paper)-[:presented_in]->(otherConf:Conference)
    # WHERE otherConf.name <> conference
    # WITH conference, author, count(DISTINCT otherConf.name) AS edition_count
    # WHERE edition_count >= 4
    # RETURN conference, collect(DISTINCT author) AS community
    results2 = session.run(query2)

    data = []
    for record in results2:
        data.append({"Conference": record['conference'], "Community Members": record['community']})

    write_to_csv(data, "conference_communities.csv")
    print(f"Conference Communities written to conference_communities.csv")


# Find the impact factors of the journals in your graph
# // To calculate impact factor, we will count the number of citations in a paper that was published in year0. Year0 will be pipelined down to the next match to count the number of publications in each journal in Year0-1, and then again in Year0-2. These values are used to finally calculate impact factor. Any journal and year combinations where a year0, year1, and year2 combaination do not exist are automatically disqualified from being calculated as a match will not exist. So the RETURN will only provide values where there were publications and citations in all years.
def find_if(session):
    """
    This function calculates and retrieves the impact factor for each journal in the graph.
    """
    query = """
    MATCH ()-[cite:CITE]->(p0:Paper)-[:published_in]->(jnl:Journal)-[:IN_YEAR]-(y0:Year)
    MATCH (p0)-[:PUBLISHED_IN]->(y0)
    WITH jnl, y0, count(cite) as cite_num

    MATCH (p1:Paper)-[:IN_COLLECTION]->(jnl)
    MATCH (p1)-[pub1:PUBLISHED_IN]->(y1:Year)
        WHERE y1.year = y0.year - 1
    WITH jnl, y0, cite_num, y1, count(pub1) AS pub_num1

    MATCH (p2:Paper)-[:IN_COLLECTION]->(jnl)
    MATCH (p2)-[pub2:PUBLISHED_IN]->(y2)
        WHERE y2.year = y0.year - 2
    WITH jnl, y0, y1, y2, cite_num, pub_num1, count(pub2) AS pub_num2

    RETURN jnl AS Journal, y0 AS ImpactFactorYear, cite_num/(pub_num1+pub_num2) AS ImpactFactor

    """
    # MATCH ()-[cite:CITED]->(p0:Paper)-[:IN_COLLECTION]->(jnl:Journal)-[:IN_YEAR]-(y0:Year)
    # MATCH (p0)-[:PUBLISHED_IN]->(y0)
    # WITH jnl, y0, count(cite) AS cite_num

    # MATCH (p1:Paper)-[:IN_COLLECTION]->(jnl)
    # MATCH (p1)-[:PUBLISHED_IN]->(y1:Year)
    # WHERE y1.year = y0.year - 1
    # WITH jnl, y0, cite_num, y1, count(p1) AS pub_num1

    # MATCH (p2:Paper)-[:IN_COLLECTION]->(jnl)
    # MATCH (p2)-[:PUBLISHED_IN]->(y2:Year)
    # WHERE y2.year = y0.year - 2
    # WITH jnl AS journal, y0, y1, y2, cite_num, pub_num1, count(p2) AS pub_num2

    # WHERE pub_num1 > 0 AND pub_num2 > 0  # Ensure publications in both previous years
    # RETURN journal.journal_name AS Journal, y0.year AS ImpactFactorYear, cite_num/(pub_num1+pub_num2) AS ImpactFactor
    results = session.run(query)

    data = []
    for record in results:
        data.append({"Journal": record['Journal'], "Impact Factor Year": record['ImpactFactorYear'], "Impact Factor": record['ImpactFactor']})

    write_to_csv(data, "journal_impact_factors.csv")
    print(f"Journal Impact Factors written to journal_impact_factors.csv")


# Find the h-indexes of the authors in your graph
def find_hindex(session):
    query4 = """
    MATCH (author:Authors)<-[:written_by]-(paper:Paper)
    RETURN author, collect(paper) AS papers
    """
    results4 = session.run(query4)

    data = []
    for record in results4:
        author = record['author']
        papers = record['papers']

        citation_list = []
        for paper in papers:
            result = session.run("""
            MATCH (paper:Paper {doi: "{paperDoi}"})
            WITH count(*) AS citations
            RETURN citations
            """.format(paperDoi=paper.doi))
            citation_list.append(result.single()['citations'])

        citation_list.sort(reverse=True)
        h_index = 0
        for citations in citation_list:
            if citations <= h_index:
                break
            h_index += 1

        data.append({"Author": author['name'], "h-index": h_index})

    write_to_csv(data, "author_h_indexes.csv")
    print(f"Author H-Indexes written to author_h_indexes.csv")


def main():
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    # Account of Linhan: 
    # URI = "neo4j+s://5b7afbed.databases.neo4j.io"
    # AUTH = ("neo4j", "BCtlDMBoyBR-gaaWJejbwe9tI2YlQ9S6_VDD2_dRT1c")
    # Account of Ziyong:
    URI = "neo4j+s://2c8207ff.databases.neo4j.io"
    AUTH = ("neo4j", "B_YGrnwwnkPrbikiT3MaQ_SG9khS7ICupTiT8mLQCVA")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print("connection successful!")
        with driver.session(database="neo4j") as session:
                session.execute_write(find_top3_cited)
                # session.execute_write(find_commu)
                # session.execute_write(find_if)
                # session.execute_write(find_hindex)
                print('Results output successfully!')

if __name__ == "__main__":
     main()