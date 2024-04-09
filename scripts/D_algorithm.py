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

def algorithm_louvain(session):
    session.run("""CALL gds.graph.drop('louvain_graph',false);""")

    session.run(
         """CALL gds.graph.project(
            'louvain_graph',
            'Authors',
            {
                written_by: {
                    orientation: 'UNDIRECTED'
                }
            })"""
                )
    result = session.run(
         """CALL gds.louvain.stream('louvain_graph')
            YIELD nodeId, communityId, intermediateCommunityIds
            RETURN gds.util.asNode(nodeId).author_name AS Author, communityId
            ORDER BY Author ASC
            LIMIT 50"""
    )
    mutate_result = session.run(
         """CALL gds.louvain.mutate('louvain_graph', { mutateProperty: 'communityId' })
            YIELD communityCount, modularity, modularities
            RETURN communityCount, modularity, modularities"""
    )
    # Write results to CSV
    data = [["Author", "CommunityId"]]  # Header
    for record in result:
        data.append([record["Author"], record["communityId"]])
    path = os.path.join(output_path,"louvain.csv" )

    with open(path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

    # write mutate stat result
    # mutate = [["communityCount", "modularity","modularities"]]
    # for mutate_record in mutate_result:
    #      mutate.append([mutate_record["communityCount"], mutate_record["modularity"],mutate_record["modularities"]])
    # mutate_path = os.path.join(output_path, "louvain_mutate.csv")
    # with open(mutate_path, "w", newline='') as csvfile:
    #     writer = csv.writer(csvfile)
    #     writer.writerows(mutate)

    records = list(result)
    summary = result.consume()
    return records, summary

def main():
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    # Account of Linhan: 
    # URI = "neo4j+s://5b7afbed.databases.neo4j.io"
    # AUTH = ("neo4j", "BCtlDMBoyBR-gaaWJejbwe9tI2YlQ9S6_VDD2_dRT1c")
    # Account of Ziyong:
    # URI = "neo4j+s://2c8207ff.databases.neo4j.io"
    # AUTH = ("neo4j", "B_YGrnwwnkPrbikiT3MaQ_SG9khS7ICupTiT8mLQCVA")
    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "upcsdmneo4j")
    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        print("connection successful!")
        with driver.session(database="neo4j") as session:
                print('Algorithm Node Similarity Ongoing')
                session.execute_write(algorithm_node_similarity)
                # session.execute_write(algorithm_louvain)
                print('Results output successfully!')

if __name__ == "__main__":
     main()