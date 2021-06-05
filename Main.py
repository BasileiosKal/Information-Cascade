from MonteCarlo_utils import Monte_Carlo
from utils import *
import multiprocessing as mp
import sqlite3
import time
import datetime
from tqdm import tqdm


if __name__ == "__main__":
    # The SG_2.db database path
    SGdb_PATH = 'C:/Users/gauss/Downloads/SG_2.db'
    # The path to the db to save the reliable sets
    RSdb_PATH = "C:/Users/gauss/Documents/Cs Master/Projects/Social Networks/PyCharm Project/Databases/RS_1v1.db"

    CATEGORY_TABLE = "SG_10"          # Table corresponding to the category we will create a graph for
    RUNS = 1000                       # Monte Carlo simulations
    THRESHOLD = 0.01
    SUBGRAPH_SIZE = 10000

    pbar = tqdm(total=10)     # Progress bar

    # Function to gather the results from each thread running in parallel
    def collect_result(result):
        global results
        global pbar
        results.append(result)
        pbar.update()

    # ====================================================================================================== #
    # Create the graphs
    # ====================================================================================================== #
    SGdb = sqlite3.connect(SGdb_PATH, uri=True)

    # Create main graph
    MainGraph = get_graph(CATEGORY_TABLE, SGdb)
    SGdb.close()
    # Get subgraph
    supgraph = get_supgraph2(MainGraph, supgraph_size=SUBGRAPH_SIZE)
    # Create users list here for faster iteration later
    users_in_graph = [uid for uid in supgraph.nodes]

    # ************************************************************** #
    # Transfer the Graph from Networkx to dictionaries to be passed
    # on the Monte_carlo function. That is necessary as a
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
    start_time = time.time()  # To count the total running time

    print("Number of processors: ", mp.cpu_count())

    pool = mp.Pool(mp.cpu_count())    # for the parallel run

    print("Starting at: ", datetime.datetime.now())
    print("Running...", end=" ")

    # -----> Using apply
    # results = [pool.apply(Monte_carlo_parallel_apply, args = (user, runs, edges_weights, runs*threshold)) for user in users_in_graph]

    # -----> Using apply async
    results = []
    for user in users_in_graph:
        pool.apply_async(Monte_Carlo, args=(user, RUNS, dict_puvi, RUNS*THRESHOLD), callback = collect_result)

    # -----> Using map
    # pool.map_async(Monte_carlo_parallel_map, [user for user in users_in_graph], callback=collect_result)
    # results = pool.map_async(Monte_carlo_Map_parallel, [user for user in users_in_graph]).get()

    # -----> Using starmap_async
    # results = pool.starmap_async(Monte_Carlo, [(user, RUNS, dict_puvi, RUNS*THRESHOLD) for user in users_in_graph]).get()

    pool.close()
    pool.join()

    end_time = time.time()

    print()
    print("Done")
    print("===================================")
    print("Running Time: ", end_time - start_time)

    time_in_min = end_time - start_time
    time_in_min = time_in_min / 60

    print("Required time in min: ", time_in_min)
    print("Required time in hours: ", time_in_min / 60)

    # Create dictionary with reliable sets
    # keys: user id, values: reliable set of user
    ReliableSets2 = {}
    for res in results:
        user = res[0]
        RS = res[1]
        ReliableSets2[user] = RS

    # ====================================================================================================== #
    # Reliable sets Stats
    # ====================================================================================================== #
    print("=======================================================================================================")
    print("=======================================================================================================")
    print("=======================================================================================================")

    get_reliable_sets_stats(ReliableSets2)
