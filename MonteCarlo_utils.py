from collections import defaultdict
import time
import numpy as np


# Suppose graph is a dictionary like the you created from trust
def reachable_set(graph, source):
    R = []
    R = find_path(graph, source, R)
    return R


def find_path(graph, source, R):
    # print(graph)
    R.append(source)
    if source not in graph.keys():  # Use graph.nodes if graph uses networkx
        return R

    for node in graph[source]:
        if node not in R:
            find_path(graph, node, R)
    return R


def Monte_Carlo(userid, T, dict_puvi, prob_threshold=0.0):

    edges_with_weights = []
    for user1 in dict_puvi.keys():
        for user2 in dict_puvi[user1]:
            edges_with_weights.append((user1, user2, dict_puvi[user1][user2]))


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



def run_MonteCarlo(supgraph, runs=100, threshold=0.001):  # runs: Monte Carlo trials
    # graph = "SG_3"
    # users_in_graph = SGdb.execute("select distinct uid from "+graph)

    ReliableSets = {}

    # Create users list here for faster iteration later
    users_in_graph = [uid for uid in supgraph.nodes]

    start_time = time.time()  # To count the total time
    per_process_start_time = time.time()  # to count the time for 10 itterations

    progress_count = 0

    print("Progress (time): ", end=',')
    for user in users_in_graph:
        # user = user[0]
        # print(user)

        reliable_set = Monte_Carlo(runs, supgraph, user, prob_threshold=runs * threshold)
        ReliableSets[user] = reliable_set

        # Print the process (count, time it took)
        if progress_count % 10 == 0:
            per_process_end_time = time.time()
            print(
                "{count} ({time:.2f})".format(count=progress_count, time=per_process_end_time - per_process_start_time),
                end=" ")
            per_process_start_time = time.time()

        progress_count += 1

    end_time = time.time()

    print()
    print("Done")
    print("===================================")
    print("Runing Time: ", end_time - start_time)

    time_in_min = end_time - start_time
    time_in_min = time_in_min / 60

    print("Required time in min: ", time_in_min)
    print("Required time in hours: ", time_in_min / 60)

    return ReliableSets



def visit_neighbors(graph, nodesList, R, NodesVisited):
    """visit all the neighbors of the nodes in
    nodesList"""
    to_visit_next = []
    for node in nodesList:
        if not NodesVisited[node]:
            NodesVisited[node] = True

            for neighbor in graph[node]:
                to_visit_next.append(neighbor)
                if neighbor not in R:
                    R = np.append(R, neighbor)

    return R, to_visit_next



def near_reachable_nodes(graph, source, max_path_length=4):
    """Create a list with all the nodes reachable from the source with a path
    of at most max_path_length edges"""
    # Suppose graph is a dictionary like the you created from trust
    count = 0
    R = np.array([])
    nodesList = [source]
    R = []

    NodesVisited = {}
    for node in graph.keys():
        NodesVisited[node] = False

    while count <= max_path_length:
        nextR, next_nodes_list = visit_neighbors(graph, nodesList, R, NodesVisited)
        R = nextR
        nodesList = next_nodes_list
        count += 1

    return R
