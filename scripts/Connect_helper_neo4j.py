from neo4j import GraphDatabase

def delete_and_detach_all_nodes(session):
    session.run(
        "MATCH (n) DETACH DELETE n"
    )

def create_session():
    ############### Can change to your own URI, username and pwd here ###############
    URI = "neo4j+s://2c8207ff.databases.neo4j.io"
    username = 'neo4j'
    password = 'B_YGrnwwnkPrbikiT3MaQ_SG9khS7ICupTiT8mLQCVA'

    print('Creating neo4j connection...')
    driver = GraphDatabase.driver(URI, auth=(username, password))

    session = driver.session()
    print('Connection success, session initiated...')

    return session

def clean_session(session):
    
    print('Deleting and detaching all the previous nodes in the database.')
    session.write_transaction(delete_and_detach_all_nodes)
    print('Clean session successfully.')
    return session