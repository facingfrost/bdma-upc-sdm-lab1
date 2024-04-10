
from neo4j import GraphDatabase
import os
import csv

def write_to_csv(data, filename):
    output_path = '/Users/wanglinhan/Desktop/BDMA/UPC/SDM/labs/bdma-upc-sdm-lab1/recommendation_res'
    file_path = os.path.join(output_path, filename)
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if data:  # Write header only if data is not empty
            writer.writerow([key for key in data[0].keys()])
        for row in data:
            writer.writerow(row.values())

# The first thing to do is to find/define the research communities.
find_community_query = """
MATCH (p:Paper)-[r:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
          WHERE toLower(k.keywords) CONTAINS toLower(keyword))
RETURN DISTINCT p
"""

# Next, we need to find the conferences and journals related to the database community
find_related_jour_conf = """
// Calculate the total number of papers presented in each conference
MATCH (c:Conference)<-[:presented_in]-(p:Paper)
WITH c, COUNT(p) AS totalPapers

MATCH (c)<-[:presented_in]-(p:Paper)-[:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
          WHERE toLower(k.keywords) CONTAINS toLower(keyword))
WITH c, COUNT(DISTINCT p) AS papersWithKeywords, totalPapers

// Calculate the percentage of papers with keywords presented in each conference
WITH c, papersWithKeywords, totalPapers, (toFloat(papersWithKeywords) / toFloat(totalPapers) * 100) AS percentageWithKeywords

// Filter conferences where the percentage of papers with keywords is greater than or equal to 90%
WHERE percentageWithKeywords >= 90

// Return the desired results
RETURN c AS conference_or_journal, percentageWithKeywords


UNION

// Calculate the total number of papers presented in each conference
MATCH (j:Journal)<-[:published_in]-(p:Paper)
WITH j, COUNT(p) AS totalPapers

MATCH (j)<-[:published_in]-(p:Paper)-[:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
          WHERE toLower(k.keywords) CONTAINS toLower(keyword))
WITH j, COUNT(DISTINCT p) AS papersWithKeywords, totalPapers

// Calculate the percentage of papers with keywords presented in each conference
WITH j, papersWithKeywords, totalPapers, (toFloat(papersWithKeywords) / toFloat(totalPapers) * 100) AS percentageWithKeywords

// Filter conferences where the percentage of papers with keywords is greater than or equal to 90%
WHERE percentageWithKeywords >= 90

// Return the desired results
RETURN j AS conference_or_journal, percentageWithKeywords

"""

# Next, we want to identify the top papers of these conferences/journals.
find_related_jour_conf_topcited = """
// Find conferences and journals with keywords
MATCH (c:Conference)<-[:presented_in]-(p:Paper)
WITH c, COUNT(p) AS totalPapers
MATCH (c)<-[:presented_in]-(p:Paper)-[:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
        WHERE toLower(k.keywords) CONTAINS toLower(keyword))
WITH c, COUNT(DISTINCT p) AS papersWithKeywords, totalPapers
WITH c, papersWithKeywords, totalPapers, (toFloat(papersWithKeywords) / toFloat(totalPapers) * 100) AS percentageWithKeywords
WHERE percentageWithKeywords >= 90
WITH COLLECT(c) AS conferences_community

MATCH (j:Journal)<-[:published_in]-(p:Paper)
WITH j, COUNT(p) AS totalPapers, conferences_community
MATCH (j)<-[:published_in]-(p:Paper)-[:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
        WHERE toLower(k.keywords) CONTAINS toLower(keyword))
WITH j, COUNT(DISTINCT p) AS papersWithKeywords, totalPapers, conferences_community
WITH j, papersWithKeywords, totalPapers, (toFloat(papersWithKeywords) / toFloat(totalPapers) * 100) AS percentageWithKeywords, conferences_community
WHERE percentageWithKeywords >= 90
WITH conferences_community, COLLECT(j) AS journals_community

// Collect papers presented in conferences
MATCH (c:Conference)<-[:presented_in]-(paper:Paper)
WHERE c IN conferences_community
WITH COLLECT(paper) AS paper_conference, journals_community

// Collect papers published in journals
MATCH (j:Journal)<-[:published_in]-(paper:Paper)
WHERE j IN journals_community
WITH COLLECT(paper) AS paper_journals, paper_conference

// Find the most cited paper among conferences and journals
MATCH (paper:Paper)
WHERE (paper IN paper_conference) OR (paper IN paper_journals)
WITH paper, [(paper)<-[:cites]-() | 1] AS citationCountList
WITH paper, size(citationCountList) AS citationCount
ORDER BY citationCount DESC
LIMIT 100

// Return the most cited paper
RETURN paper AS most_cited_paper, citationCount
"""

# Finally, an author of any of these top-100 papers is automatically considered a potential good match to review database papers.
find_reviewers = """
// Find conferences and journals with keywords
MATCH (c:Conference)<-[:presented_in]-(p:Paper)
WITH c, COUNT(p) AS totalPapers
MATCH (c)<-[:presented_in]-(p:Paper)-[:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
        WHERE toLower(k.keywords) CONTAINS toLower(keyword))
WITH c, COUNT(DISTINCT p) AS papersWithKeywords, totalPapers
WITH c, papersWithKeywords, totalPapers, (toFloat(papersWithKeywords) / toFloat(totalPapers) * 100) AS percentageWithKeywords
WHERE percentageWithKeywords >= 90
WITH COLLECT(c) AS conferences_community

MATCH (j:Journal)<-[:published_in]-(p:Paper)
WITH j, COUNT(p) AS totalPapers, conferences_community
MATCH (j)<-[:published_in]-(p:Paper)-[:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
        WHERE toLower(k.keywords) CONTAINS toLower(keyword))
WITH j, COUNT(DISTINCT p) AS papersWithKeywords, totalPapers, conferences_community
WITH j, papersWithKeywords, totalPapers, (toFloat(papersWithKeywords) / toFloat(totalPapers) * 100) AS percentageWithKeywords, conferences_community
WHERE percentageWithKeywords >= 90
WITH conferences_community, COLLECT(j) AS journals_community

// Collect papers presented in conferences
MATCH (c:Conference)<-[:presented_in]-(paper:Paper)
WHERE c IN conferences_community
WITH COLLECT(paper) AS paper_conference, journals_community

// Collect papers published in journals
MATCH (j:Journal)<-[:published_in]-(paper:Paper)
WHERE j IN journals_community
WITH COLLECT(paper) AS paper_journals, paper_conference

// Find the most cited paper among conferences and journals
MATCH (paper:Paper)
WHERE (paper IN paper_conference) OR (paper IN paper_journals)
WITH paper, [(paper)<-[:cites]-() | 1] AS citationCountList
WITH paper, size(citationCountList) AS citationCount
ORDER BY citationCount DESC
LIMIT 100

WITH COLLECT(paper) as top_papers
MATCH (paper:Paper)-[:written_by]->(author:Authors)
WHERE paper IN top_papers
RETURN DISTINCT(author) as authors
"""

#  In addition, we want to identify gurus
find_gurus = """
// Find conferences and journals with keywords
MATCH (c:Conference)<-[:presented_in]-(p:Paper)
WITH c, COUNT(p) AS totalPapers
MATCH (c)<-[:presented_in]-(p:Paper)-[:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
        WHERE toLower(k.keywords) CONTAINS toLower(keyword))
WITH c, COUNT(DISTINCT p) AS papersWithKeywords, totalPapers
WITH c, papersWithKeywords, totalPapers, (toFloat(papersWithKeywords) / toFloat(totalPapers) * 100) AS percentageWithKeywords
WHERE percentageWithKeywords >= 90
WITH COLLECT(c) AS conferences_community

MATCH (j:Journal)<-[:published_in]-(p:Paper)
WITH j, COUNT(p) AS totalPapers, conferences_community
MATCH (j)<-[:published_in]-(p:Paper)-[:has_keyword]->(k:Keywords)
WHERE ANY(keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying'] 
        WHERE toLower(k.keywords) CONTAINS toLower(keyword))
WITH j, COUNT(DISTINCT p) AS papersWithKeywords, totalPapers, conferences_community
WITH j, papersWithKeywords, totalPapers, (toFloat(papersWithKeywords) / toFloat(totalPapers) * 100) AS percentageWithKeywords, conferences_community
WHERE percentageWithKeywords >= 90
WITH conferences_community, COLLECT(j) AS journals_community

// Collect papers presented in conferences
MATCH (c:Conference)<-[:presented_in]-(paper:Paper)
WHERE c IN conferences_community
WITH COLLECT(paper) AS paper_conference, journals_community

// Collect papers published in journals
MATCH (j:Journal)<-[:published_in]-(paper:Paper)
WHERE j IN journals_community
WITH COLLECT(paper) AS paper_journals, paper_conference

// Find the most cited paper among conferences and journals
MATCH (paper:Paper)
WHERE (paper IN paper_conference) OR (paper IN paper_journals)
WITH paper, [(paper)<-[:cites]-() | 1] AS citationCountList
WITH paper, size(citationCountList) AS citationCount
ORDER BY citationCount DESC
LIMIT 100

WITH COLLECT(paper) as top_papers

// Match authors who are authors of at least two papers and are in the top papers
MATCH (a:Authors)<-[:written_by]-(p:Paper)
WHERE p IN top_papers
WITH a, count(p) as countp
WHERE countp > 1
RETURN a
"""


def main():
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j+s://5b7afbed.databases.neo4j.io"
    AUTH = ("neo4j", "BCtlDMBoyBR-gaaWJejbwe9tI2YlQ9S6_VDD2_dRT1c")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print("connection successful!")
        with driver.session(database="neo4j") as session:
                community = session.run(find_community_query).data()
                print('find_community_query Success!')
                data = []
                for record in community:
                    data.append({"Year": record["p"]['year'], "link": record["p"]['link'], "abstract": record["p"]["abstract"], "doi": record["p"]["doi"]})
                write_to_csv(data, "find_community.csv")
                print(f"find_community.csv written")
                related_conf = session.run(find_related_jour_conf).data()
                print("find_related_jour_conf success!")
                data = []
                for record in related_conf:
                    data.append({"conference_name": record['conference_or_journal'].get("name",0), "journal_name": record['conference_or_journal'].get("journal_name",0)})
                write_to_csv(data, "find_jour_conf.csv")
                print(f"find_jour_conf.csv written")
                top_cited = session.run(find_related_jour_conf_topcited).data()
                print("top cited found!")
                data = []
                for record in top_cited:
                    data.append({"Year": record['most_cited_paper']['year'], "link": record['most_cited_paper']['link'], "abstract": record['most_cited_paper']["abstract"], "doi": record['most_cited_paper']["doi"]})
                write_to_csv(data, "top_cited.csv")
                reviewers = session.run(find_reviewers).data()
                print("find reviewers success!")
                data = []
                for record in reviewers:
                    data.append({"author_name": record['authors']['author_name'], "author_id": record['authors']['author_id']})
                write_to_csv(data, "reviewers.csv")
                gurus = session.run(find_gurus).data()
                print("find gurus success!")
                data = []
                for record in gurus:
                    data.append({"author_name": record['a']['author_name'], "author_id": record['a']['author_id']})
                write_to_csv(data, "gurus.csv")

if __name__ == "__main__":
     main()