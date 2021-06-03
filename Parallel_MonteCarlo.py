from MonteCarlo_utils import *
from utils import *
from collections import defaultdict
import numpy as np
import multiprocessing as mp



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


if __name__ == "__main__":
    SGdb = sqlite3.connect('C:/Users/gauss/Downloads/SG_2.db', uri=True)
    table_of_category = "SG_1"  # Table corresponding to the category we will create a graph for
    # Create main graph
    MainGraph = get_graph(table_of_category, SGdb)
    SGdb.close()
    # Get sup graph
    supgraph = get_supgraph2(MainGraph, supgraph_size=1000)
    # Get the edges with the weights of the subgraph
    edges_weights = [(user1, user2, puvi) for (user1, user2, puvi) in supgraph.edges.data('weight')]
    # Create users list here for faster iteration later
    users_in_graph = [uid for uid in supgraph.nodes]

    # ====================================================================================================== #
    # Helping Functions
    # ====================================================================================================== #
    def Monte_carlo_parallel_map(userid, T=100, edges_with_weights=edges_weights, prob_threshold=2):
        # Create the random numbers list here for speed when iterating later
        rnd_list = np.random.uniform(0, 1, size=((len(edges_with_weights)) * T,))
        i = 0

        c = defaultdict(lambda: 0)  # dictionary to store the counters for each userid
        for h in range(T):  # T = Number of samples
            R_tmp = []  # R_tmp = Rh = all nodes reachable from i in the graph Gh

            G_tmp = {}  # G_tmp = the graph  that is created in h iteration as a DICTIONARY
            # G_tmp = nx.Graph()

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

    def collect_result(result):
        global results
        results.append(result)

    runs = 100
    threshold = 0.02
    # ReliableSets = {}
    results = []


    # ====================================================================================================== #
    # Run Monte Carlo
    # ====================================================================================================== #
    start_time = time.time()  # To count the total time
    pool = mp.Pool(mp.cpu_count())

    print("Runing...", end=" ")
    # Using apply
    # results = [pool.apply(Monte_carlo_parallel, args = (runs, edges_with_weights, user, runs*threshold)) for user in users_in_graph]

    # Using map
    pool.map_async(Monte_carlo_parallel_map, [user for user in users_in_graph], callback=collect_result)

    # results = pool.map_async(Monte_carlo_Map_parallel, [user for user in users_in_graph]).get()

    pool.close()
    pool.join()
    end_time = time.time()

    print()
    print("Done")
    print("===================================")
    print("Runing Time: ", end_time - start_time)

    time_in_min = end_time - start_time
    time_in_min = time_in_min / 60

    print("Required time in min: ", time_in_min)
    print("Required time in hours: ", time_in_min / 60)

    # ====================================================================================================== #
    # RResults Stats
    # ====================================================================================================== #
    print("=======================================================================================================")
    ReliableSets2 = {}
    for res in results[0]:
        user = res[0]
        RS = res[1]
        # print(RS)
        ReliableSets2[user] = RS

    lengths_of_Ris2 = []
    for RI in ReliableSets2.values():
        # print(RI)
        lengths_of_Ris2.append(len(RI))

    print("Reliable sets stats:")
    print("----------------------------------------------------")
    print("Number of reliable sets: ", len(ReliableSets2.values()))
    print("Average size: ", (sum(lengths_of_Ris2)) / len(lengths_of_Ris2))
    print("Max size", max(lengths_of_Ris2))
    print("Number of reliable sets of size>10: ", len([RS for RS in lengths_of_Ris2 if RS > 10]))
    print("Number of reliable sets of size>20: ", len([RS for RS in lengths_of_Ris2 if RS > 20]))
    print("Number of reliable sets of size>50: ", len([RS for RS in lengths_of_Ris2 if RS > 50]))
    print("----------------------------------------------------")
