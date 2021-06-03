from MonteCarlo_utils import Monte_Carlo
from utils import *
import multiprocessing as mp
import sqlite3
import time
import datetime



if __name__ == "__main__":
    CATEGORY_TABLE = "SG_2"  # Table corresponding to the category we will create a graph for
    RUNS = 10
    THRESHOLD = 0.01
    SUBGRAPH_SIZE = 10000

    def collect_result(result):
        global results
        results.append(result)

    # ====================================================================================================== #
    # Create the graphs
    # ====================================================================================================== #
    SGdb = sqlite3.connect('C:/Users/gauss/Downloads/SG_2.db', uri=True)

    # Create main graph
    MainGraph = get_graph(CATEGORY_TABLE, SGdb)
    SGdb.close()
    # Get sup graph
    supgraph = get_supgraph2(MainGraph, supgraph_size=SUBGRAPH_SIZE)
    # Create users list here for faster iteration later
    users_in_graph = [uid for uid in supgraph.nodes]

    # ************************************************************** #
    # Transfer the Graph from Networkx to dictionaries to be passed
    # on the Monte_carlo_parallel function. That is necessary as a
    # result of the way the multiprocessing module works
    # ************************************************************** #

    # the puvi dictionary
    dict_puvi = {}
    for user1 in users_in_graph:
        dict_puvi[user1] = {}
        for user2 in supgraph[user1]:
            dict_puvi[user1][user2] = supgraph[user1][user2]['weight']

    # ====================================================================================================== #
    # Run Monte Carlo
    # ====================================================================================================== #
    start_time = time.time()  # To count the total time
    print("Number of processors: ", mp.cpu_count())
    pool = mp.Pool(mp.cpu_count())

    print("Starting at: ", datetime.datetime.now())
    print("Running...", end=" ")
    # Using apply
    # results = [pool.apply(Monte_carlo_parallel_apply, args = (user, runs, edges_weights, runs*threshold)) for user in users_in_graph]

    # -----> Using apply async
    # for user in users_in_graph:
    #     pool.apply_async(Monte_carlo_parallel_apply, args=(user, runs, edges_weights, runs * threshold), callback = collect_result)

    # -----> Using map
    # pool.map_async(Monte_carlo_parallel_map, [user for user in users_in_graph], callback=collect_result)

    # results = pool.map_async(Monte_carlo_Map_parallel, [user for user in users_in_graph]).get()

    # -----> Using starmap_async
    results = pool.starmap_async(Monte_Carlo, [(user, RUNS, dict_puvi, RUNS*THRESHOLD) for user in users_in_graph]).get()

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

    get_reliable_sets_stats(ReliableSets2)

