
from neo4j import GraphDatabase

def evolve_graph(session):
    # add accept_possibility
    session.run(
         """
        LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/author_review.csv' AS relation
        MATCH (paper:Paper {doi: relation.paper_id})
        MATCH (reviewer:Authors {author_id: relation.reviewer_id})
        MERGE (paper)-[r:reviewed_by]->(reviewer)
        SET r.accept_possibility = toFloat(relation.accept_possibility)"""
    )

    # add author_affiliation
    session.run(
         """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/authors.csv' AS node
         MATCH (author:Authors {author_id: node.author_id})
         set author.affiliation = node.author_affiliation
"""
    )


def main():
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j+s://"
    AUTH = ("neo4j", "your_password")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print("connection successful!")
        with driver.session(database="neo4j") as session:
                session.execute_write(evolve_graph)
                print('Evolve done for the database.')


if __name__ == "__main__":
     main()