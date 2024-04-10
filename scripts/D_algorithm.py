from neo4j import GraphDatabase
import csv
import os

output_path = "/Users/zzy13/Desktop/Classes_at_UPC/SDM_Semantic_data_management/Lab_1/Codes/Data/query_result"
# Algorithm1 --- Node Similarity --- DONE
# According to the manual: 
# https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/

def algorithm_node_similarity(session):
    session.run("""CALL gds.graph.drop('node_similarity_graph',false);""")

    session.run(
        """CALL gds.graph.project(
            'node_similarity_graph', 
            ['Paper','Keywords'], 
            'has_keyword');"""
    )

    session.run(
        """CALL gds.nodeSimilarity.write.estimate('node_similarity_graph', {
              writeRelationshipType: 'SIMILAR',
              writeProperty: 'score'
            });"""
    )
    # Stream result
    result = session.run(
        """CALL gds.nodeSimilarity.stream('node_similarity_graph')
            YIELD node1, node2, similarity
            RETURN gds.util.asNode(node1).title AS Paper1, 
                gds.util.asNode(node2).title AS Paper2, 
                similarity
            ORDER BY similarity DESCENDING, Paper1, Paper2
            LIMIT 20;"""
    )
    # Write results to CSV
    data = [["Paper1", "Paper2", "Similarity"]]  # Header
    for record in result:
        data.append([record["Paper1"], record["Paper2"], record["similarity"]])
    path = os.path.join(output_path,"node_similarity.csv" )

    with open(path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)
    records = list(result)
    summary = result.consume()
    # Top result for each person
    top1_result = session.run(
         """CALL gds.nodeSimilarity.stream('node_similarity_graph', { topK: 1 })
            YIELD node1, node2, similarity
            RETURN gds.util.asNode(node1).title AS Paper1, gds.util.asNode(node2).title AS Paper2, similarity
            ORDER BY Paper1 ASC
            LIMIT 20 """
    )
    top1_data = [["Paper1", "Paper2", "Similarity"]]  # Header
    for top in top1_result:
        top1_data.append([top["Paper1"], top["Paper2"], top["similarity"]])
    path = os.path.join(output_path,"top1_node_similarity.csv" )
    with open(path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(top1_data)
    
    return records, summary
# Algorithm2 --- Louvain method
# According to the manual: 
# https://neo4j.com/docs/graph-data-science/current/algorithms/louvain/

def algorithm_louvain_paper(session):
    session.run("""CALL gds.graph.drop('louvain_graph_paper',false);""")

    session.run(
         """CALL gds.graph.project(
            'louvain_graph_paper',
            'Paper',
            {
                cites: {
                    orientation: 'UNDIRECTED'
                }
            })"""
                )
    result = session.run(
         """CALL gds.louvain.stream('louvain_graph_paper')
            YIELD nodeId, communityId, intermediateCommunityIds
            RETURN gds.util.asNode(nodeId).title AS Paper, communityId
            ORDER BY communityId ASC
            LIMIT 50"""
    )

    # Write results to CSV
    data = [["Paper", "CommunityId"]]  # Header
    for record in result:
        data.append([record["Paper"], record["communityId"]])
    path = os.path.join(output_path,"louvain_paper.csv" )

    with open(path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

    records = list(result)
    summary = result.consume()
    return records, summary

def main():
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j+s://"
    AUTH = ("neo4j", "your_password")
    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print("connection successful!")
        with driver.session(database="neo4j") as session:
                print('Algorithm Node Similarity Ongoing')
                session.execute_write(algorithm_node_similarity)
                print('Algorithm Louvain Ongoing')
                session.execute_write(algorithm_louvain_paper)
                print('Results output successfully!')

if __name__ == "__main__":
     main()