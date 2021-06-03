from Parallel_MonteCarlo import Monte_carlo_parallel_apply
from utils import *
import multiprocessing as mp
import sqlite3
import time
import datetime


if __name__ == "__main__":
    RUNS = 1000   # Monte Carlo simulations
    THRESHOLD = 0.01   # Threshold of Monte Carlo algorithm
    SUBGRAPH_SIZE = 10000
    MAX_PATH_LENGTH = 3   # The maximum distance from the source node that a user can be and be considered to be in the
                          # source nodes reachable set.

    def collect_result(result):
        global results
        results.append(result)

    # ====================================================================================================== #
    # Create the graphs
    # ====================================================================================================== #
    SGdb = sqlite3.connect('C:/Users/gauss/Downloads/SG_2.db', uri=True)
    table_of_category = "SG_1"  # Table corresponding to the category we will create a graph for
    # Create main graph
    MainGraph = get_graph(table_of_category, SGdb)
    SGdb.close()
    # Get sup graph
    supgraph = get_supgraph2(MainGraph, supgraph_size=SUBGRAPH_SIZE)
    # Get the edges with the weights of the subgraph
    # edges_weights = [(user1, user2, puvi) for (user1, user2, puvi) in supgraph.edges.data('weight')]
    # Create users list here for faster iteration later
    users_in_graph = [uid for uid in supgraph.nodes]

    print("Number of processors: ", mp.cpu_count())


    # ====================================================================================================== #
    # Run Monte Carlo
    # ====================================================================================================== #
    print("Starting at: ", datetime.datetime.now())
    # start_time = time.time()  # To count the total time

    # Adjacency list of the sup graph
    dict_subgraph = {}
    for node in supgraph.nodes():
        dict_subgraph[node] = list(supgraph.neighbors(node))

    dict_puvi = {}
    for user1 in supgraph.nodes():
        dict_puvi[user1] = {}
        for user2 in supgraph[user1]:
            dict_puvi[user1][user2] = supgraph[user1][user2]['weight']

    # # For each user create a smaller set in which to run the Monte Carlo algorithm
    # print("Calculating nearby sets ... ", end=" ")
    # nearby_graph_start_time = time.time()
    # surrounding_graph = {}   # Keys: user_id, values: the nodes near that user
    # for source_user in users_in_graph:
    #     # the nodes close to the source_user
    #     visited = near_reachable_nodes(dict_subgraph, source_user, max_path_length=MAX_PATH_LENGTH)
    #     # Create the subgraph with the nodes close the the source_user
    #     users_subgraph = supgraph.subgraph(visited)
    #     # Get the edges and the weights (probabilities of influence: "puvi")
    #     near_edges_weights = [(user1, user2, puvi) for (user1, user2, puvi) in users_subgraph.edges.data('weight')]
    #     surrounding_graph[source_user] = near_edges_weights

    # nearby_graph_end_time = time.time()
    # print("Finish after: {m} min".format(m = (nearby_graph_end_time-nearby_graph_start_time)/60))

    start_time = time.time()
    print("Running Monte Carlo...", end=" ")

    pool = mp.Pool(mp.cpu_count())

    #results = []
    #for user in users_in_graph:
    #    pool.apply_async(Monte_carlo_parallel_apply, args=(user, RUNS, supgraph, dict_subgraph, RUNS*THRESHOLD, MAX_PATH_LENGTH), callback = collect_result)

    # -----> Using starmap_async
    prob_threshold = RUNS*THRESHOLD
    results = pool.starmap_async(Monte_carlo_parallel_apply, [(user, RUNS, dict_puvi, dict_subgraph, prob_threshold, MAX_PATH_LENGTH) for user in users_in_graph]).get()

    pool.close()
    # pool.join()

    end_time = time.time()

    print()
    print("Done")
    print("===================================")
    print("Running Time: ", end_time - start_time)

    time_in_min = end_time - start_time
    time_in_min = time_in_min / 60

    print("Required time in min: ", time_in_min)
    print("Required time in hours: ", time_in_min / 60)

    # ====================================================================================================== #
    # RResults Stats
    # ====================================================================================================== #
    print("=======================================================================================================")
    print("=======================================================================================================")
    print("=======================================================================================================")
    ReliableSets2 = {}
    for res in results:
        user = res[0]
        RS = res[1]
        # print(RS)
        ReliableSets2[user] = RS

    lengths_of_Ris2 = []
    for RI in ReliableSets2.values():
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
