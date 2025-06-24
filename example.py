from hot_vectors import *
import random as r

import matplotlib.pyplot as plt

if __name__ == "__main__":
    with HotVectorClient('127.0.0.1', 3000) as client:
        print(client.create_cluster(0.1))

        # # generate 3 clusters with 10 vectors each
        cluster_centroids = [
            [r.random() * 5. for _ in range(384)] for _ in range(4)
        ]

        for i, centroid in enumerate(cluster_centroids):
            for i2 in range(r.randint(20, 50)):
                vector = [centroid[0] + r.random() for _ in range(384)]
                print(client.send_vector(vector))
        # for _ in range(10):
        #     print(client.send_vector([r.random(), r.random(), r.random()]))

        partitions = client.get_partition_meta_data()
        print(partitions)

        print("\n\n\n")

        for meta in partitions:
            
            print(f"\nPartitionId({meta.id})")

            partition_vectors = client.get_vectors(PartitionId(meta.id))

            print(partition_vectors)

            vectors = []
            for vector in partition_vectors:
                vectors+=client.get_vectors(VectorId(vector.id))

            assert vectors == partition_vectors, f"Mismatch for PartitionId({meta.id})"

            print("\n\n")
        
        print("\n\n")

        cluster_meta = client.get_cluster_meta_data(0.1)

        for meta in cluster_meta:
            print(f"\nClusterId({meta.id})")

            cluster_members = meta.members

            vectors = client.get_vectors(ClusterId(0.1, meta.id))
            print("\n\t-".join([f"{id}" for id in vectors]))

            vector_ids = [v.id for v in vectors]
            member_ids = [vid.uuid for vid in cluster_members]

            assert sorted(vector_ids) == sorted(member_ids), f"Mismatch for ClusterId({meta.id})"

        print(client.create_cluster(5.0))
        cluster_meta = client.get_cluster_meta_data(5.0)

        for meta in cluster_meta:
            print(f"\nClusterId({meta.id})")

            cluster_members = meta.members

            vectors = client.get_vectors(ClusterId(5.0, meta.id))
            print("\n\t-".join([f"{id}" for id in vectors]))

            vector_ids = [v.id for v in vectors]
            member_ids = [vid.uuid for vid in cluster_members]

            assert sorted(vector_ids) == sorted(member_ids), f"Mismatch for ClusterId({meta.id})"
            

        # print("\n\n")
        
        # print(client.create_cluster(0.25))

        # print("\n\n")

        # print(client.get_vectors(ClusterId(0.25, None)))

        # print("\n\n")
        
        # print(client.get_graph_data(InterEdge()))

        # # plot the graph data
        # partitions: List[Meta] = client.get_partition_meta_data()
        # print(partitions)
        # vectors: List[Vector] = []
        # for meta in partitions:
        #     vectors.extend(client.get_vectors(PartitionId(meta.id)))
        # print(vectors)

        # clusters: Dict[ClusterId, List[VectorId]] = client.get_vectors(ClusterId(0.1, None))

        # # vectors: Dict[str, List[str]]= client.get_vectors(ClusterId(0.1, None))
        # edges: List[Tuple[VectorId, VectorId]] = []
        # for meta in partitions:
        #     tmp: List[Tuple[float, VectorId, VectorId]] = client.get_graph_data(IntraEdge(PartitionId(meta.id)))
        #     edges.extend([(e[1].uuid, e[2].uuid) for e in tmp])
        # tmp: List[Tuple[float, Tuple[PartitionId, VectorId], Tuple[PartitionId, VectorId]]] = client.get_graph_data(InterEdge())
        
        # edges.extend([(e[1][1].uuid, e[2][1].uuid) for e in tmp])
        
        # print(vectors)
        # print(edges)
        # #plt 3d scatter plot of vectors and edges
        # fig = plt.figure()
        # ax = fig.add_subplot(111, projection='3d')
        # # highlight clusters
        # import matplotlib.pyplot as plt
        # from matplotlib import cm
        # import numpy as np

        # # For each cluster
        # cmap = cm.get_cmap('tab10', len(clusters))  # or use 'tab20' if more clusters
        # cluster_colors = {
        #     cluster_id: cmap(i)
        #     for i, cluster_id in enumerate(clusters.keys())
        # }

        # # Plot each cluster with a unique color
        # for cluster_id, cluster_vectors in clusters.items():
        #     cluster_vectors = [v for v in vectors if v.id in cluster_vectors]
        #     color = cluster_colors[cluster_id]

        #     ax.scatter(
        #         [v.vector[0] for v in cluster_vectors],
        #         [v.vector[1] for v in cluster_vectors],
        #         [v.vector[2] for v in cluster_vectors],
        #         label=f'Cluster {cluster_id}', alpha=0.5, color=color
        #     )

        # # Plot edges between vectors
        # for edge in edges:
        #     v1 = next(v for v in vectors if v.id == edge[0])
        #     v2 = next(v for v in vectors if v.id == edge[1])
        #     ax.plot(
        #         [v1.vector[0], v2.vector[0]],
        #         [v1.vector[1], v2.vector[1]],
        #         [v1.vector[2], v2.vector[2]],
        #         c='b'
        #     )

        # # ax.legend()
        # plt.show()

        # for cluster_id, cluster_vectors in clusters.items():
        #     cluster_vectors = [v for v in vectors if v.id in cluster_vectors]
        #     ax.scatter(
        #         [v.vector[0] for v in cluster_vectors],
        #         [v.vector[1] for v in cluster_vectors],
        #         [v.vector[2] for v in cluster_vectors],
        #         label=f'Cluster {cluster_id}', alpha=0.5
        #     )

        # ax.scatter(
        #     [v.vector[0] for v in vectors],
        #     [v.vector[1] for v in vectors],
        #     [v.vector[2] for v in vectors],
        #     c='r', marker='o'
        # )
        # for edge in edges:
        #     v1 = next(v for v in vectors if v.id == edge[0])
        #     v2 = next(v for v in vectors if v.id == edge[1])
        #     ax.plot([v1.vector[0], v2.vector[0]], [v1.vector[1], v2.vector[1]], [v1.vector[2], v2.vector[2]], c='b')
        # plt.show()

        # print("\n\n")
        # for meta in partitions:
        #     print(client.get_graph_data(IntraEdge(PartitionId(meta.id))))