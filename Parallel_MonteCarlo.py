from MonteCarlo_utils import *
from collections import defaultdict
import numpy as np



def maxPath_MonteCarlo(userid, T, dict_puvi, dict_subgraph, prob_threshold, max_distance):
    """Helping Function to run monte carlo simulations in parallel using the pool.apply function"""

    # calculate the surrounding graph of the userid
    # the nodes close to the source_user
    visited = near_reachable_nodes(dict_subgraph, userid, max_path_length=max_distance)
    # Create the subgraph with the nodes close the the source_user
    # users_subgraph = supgraph.subgraph(visited)
    # Get the edges and the weights (probabilities of influence: "puvi")

    edges_with_weights = []
    for user1 in visited:
        for user2 in dict_puvi[user1]:
            if user2 in visited:
                edges_with_weights.append((user1, user2, dict_puvi[user1][user2]))


    # edges_with_weights = surrounding_graph[userid]

    # Create the random numbers list here for speed when iterating later
    rnd_list = np.random.uniform(0, 1, size=((len(edges_with_weights)) * T,))
    i = 0

    c = defaultdict(lambda: 0)  # dictionary to store the counters for each userid
    for h in range(T):  # T = Number of samples

        G_tmp = {}  # G_tmp = the graph  that is created in h iteration as a DICTIONARY

        for user1, user2, p_uvi in edges_with_weights:
            # p_uvi = float(p_uvi)  # Cast puvi from str to float

            rnd = rnd_list[i]
            i += 1
            if rnd <= p_uvi:
                # G_tmp.add_edge(user1, user2)  # To be used if G_tmp uses networkx
                if user1 not in G_tmp.keys():
                    G_tmp[user1] = [user2]
                else:
                    G_tmp[user1].append(user2)

        R_tmp = reachable_set(G_tmp, userid)

        for v in R_tmp:
            c[v] += 1

    Ru = []
    for v in c.keys():
        if c[v] >= prob_threshold:
            Ru.append(v)
    return (userid, Ru)
