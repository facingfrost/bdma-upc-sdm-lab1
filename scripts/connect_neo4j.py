
from neo4j import GraphDatabase

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://5b7afbed.databases.neo4j.io"
AUTH = ("neo4j", "BCtlDMBoyBR-gaaWJejbwe9tI2YlQ9S6_VDD2_dRT1c")



def main():
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print("connection successful!")
        with driver.session(database="neo4j") as session:
                session.execute_write(delete_db)

def create_db(tx):
    # Create new Person node with given name, if not exists already
    tx.run("""CREATE ({Name: "", Institution: "", ScholarID: ""})-[:Contribute {CorrespondingAuthor: "Yes"}]->(` Paper` {Title: "", DOI: "", Pages_num: ""})-[:Publish_In]->({Category: "conference", Title: "", Edition: "", City: ""})-[:Year_In]->(Year)<-[:Year_In]-(` Paper`)-[:Cite]->({Name: "", DOI: ""}),
({Name: "", Institution: "", ScholarID: ""})-[:Review]->(` Paper`)-[:Publish_In]->({Title: "", Volumn: ""})-[:Year_In]->(Year),
(` Paper`)-[:Topics]->()
    """)
    print("create_db successful!")

def delete_db(tx):
     tx.run("""MATCH (n)
            DETACH DELETE n""")
     print("delete_db successful!")

if __name__ == "__main__":
     main()