
from neo4j import GraphDatabase

def delete_db(session):
     session.run("""MATCH (n)
            DETACH DELETE n""")
     print("delete_db successful!")

# load_node

def load_node_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/paper.csv' AS node
            CREATE (:Paper {
                title: node.title,
                abstract: node.abstract,
                pages: node.pages,
                doi: node.DOI,
                link: node.link
        })"""
    )

def load_node_conference(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/conference.csv' AS node
            CREATE (:Conference {
                name: node.name,
                year: toInteger(node.year),
                city: node.city
        })"""
    )

def load_node_journal(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/journal.csv' AS node
            CREATE (:Journal {
                journal_name: node.journal_name
        })"""
    )

def load_node_proceeding(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/proceeding.csv' AS node
            CREATE (:Proceeding {
                proceeding_name: node.proceeding_name
        })"""
    )

def load_node_author(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/authors.csv' AS node
            CREATE (:Authors {
                author_id: node.author_id,
                author_name: node.author_name,
                author_affiliation: node.author_institution
        })"""
    )

def load_node_keywords(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/keywords.csv' AS node
            CREATE (:Keywords {
                keywords: node.keywords
        })"""
    )

# load_relationship
    
def load_relation_conference_belong_to_proceeding(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/conference_belong_to_proceeding.csv' AS relation
            MATCH (conf:Conference {name: relation.start_id})
            WITH conf, relation
            MATCH (proceed:Proceeding {proceeding_name: relation.end_id})
            CREATE (conf)-[:belong_to]->(proceed)"""
    )

def load_relation_paper_published_in_journal(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/paper_belong_to_journal.csv' AS relation
            MATCH (paper:Paper {doi: relation.start_id})
            WITH paper, relation
            MATCH (journal:Journal {journal_name: relation.end_id})
            CREATE (paper)-[:published_in]->(journal)"""
    )

def load_relation_paper_presented_in_conference(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/paper_presented_in_conference.csv' AS relation
            MATCH (paper:Paper {doi: relation.start_id})
            WITH paper, relation
            MATCH (conf:Conference {name: relation.end_id})
            CREATE (paper)-[:presented_in]->(conf)"""
    )

def load_relation_paper_cite_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/paper_cite_paper.csv' AS relation
            MATCH (paper:Paper {doi: relation.start_id})
            WITH paper, relation
            MATCH (paperCited:Paper {doi: relation.end_id})
            CREATE (paper)-[:cites]->(paperCited)"""
    )

def load_relation_author_write(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/author_write.csv' AS relation
            MATCH (paper:Paper {doi: relation.paper_id})
            WITH paper, relation
            MATCH (author:Authors {author_id: relation.author_id})
            CREATE (paper)-[:written_by{CorrespondingAuthor: relation.corresponding}]->(author)"""
    )



def load_relation_author_review(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/author_review.csv' AS relation
            MATCH (paper:Paper {doi: relation.paper_id})
            WITH paper, relation
            MATCH (reviewer:Authors {author_id: relation.reviewer_id})
            CREATE (paper)-[:reviewed_by{review_content: relation.review_content}]->(reviewer)"""
    )

def load_relation_paper_has_keywords(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Ziyong-Zhang/SDM_Lab_1/main/Data/paper_has_keywords.csv' AS relation
            MATCH (paper:Paper {doi: relation.paper_id})
            WITH paper, relation
            MATCH (keywords:Keywords {keywords: relation.keywords})
            CREATE (paper)-[:has_keyword]->(keywords)"""
    )


def main():
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j+s://5b7afbed.databases.neo4j.io"
    AUTH = ("neo4j", "BCtlDMBoyBR-gaaWJejbwe9tI2YlQ9S6_VDD2_dRT1c")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print("connection successful!")
        with driver.session(database="neo4j") as session:
                session.execute_write(delete_db)
                print('Creating and loading the nodes and relations into the database...')
                session.execute_write(load_node_paper)
                session.execute_write(load_node_conference)
                session.execute_write(load_node_journal)
                session.execute_write(load_node_proceeding)
                session.execute_write(load_node_author)
                session.execute_write(load_node_keywords)
                session.execute_write(load_relation_conference_belong_to_proceeding)
                session.execute_write(load_relation_paper_published_in_journal)
                session.execute_write(load_relation_paper_presented_in_conference)
                session.execute_write(load_relation_paper_cite_paper)
                session.execute_write(load_relation_author_write)
                session.execute_write(load_relation_author_review)
                session.execute_write(load_relation_paper_has_keywords)
                print('Creation and loading done for the database.')


if __name__ == "__main__":
     main()