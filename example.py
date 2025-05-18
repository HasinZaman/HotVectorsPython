from hot_vectors import *
import random as r

if __name__ == "__main__":
    with HotVectorClient('127.0.0.1', 3000) as client:
        print(client.create_cluster(0.1))

        for _ in range(10):
            print(client.send_vector([r.random(),r.random()]))

        partitions = client.get_partition_meta_data()
        print(partitions)

        print("\n\n")

        for meta in partitions:
            print(client.get_vectors(PartitionId(meta.id)))
        
        print("\n\n")

        print(client.get_vectors(ClusterId(0.1, None)))

        print("\n\n")
        
        print(client.get_graph_data(InterEdge()))

        print("\n\n")
        for meta in partitions:
            print(client.get_graph_data(IntraEdge(PartitionId(meta.id))))