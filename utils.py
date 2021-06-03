from scipy.interpolate import UnivariateSpline
import networkx as nx
import numpy as np
import sqlite3
import matplotlib.pyplot as plt


def plot_pdf(probs_list, ax_obj, title, xlabel='', ylabel='Number of users'):
    N = len(probs_list)
    n = N//10
    p, x = np.histogram(probs_list, bins=n) # bin it into n = N//10 bins
    x = x[:-1] + (x[1] - x[0])/2   # convert bin edges to centers
    f = UnivariateSpline(x, p, s=n)
    ax_obj.plot(x, f(x), '-o')
    ax_obj.set_ylabel(ylabel)
    ax_obj.set_xlabel(xlabel)
    ax_obj.title.set_text(title)
    # plt.savefig("cc.png")



def get_graph(table_of_categorie, SGdb):
    """"Create the Graph of users for a category"""""
    MainGraph = nx.DiGraph()   # The networkx graph
    graph_from_db = SGdb.execute("select * from "+table_of_categorie)  # Get the saved data from the db
    # cast puvi to float
    edges_and_weights = [(user1, user2, float(puvi)) for (user1, user2, puvi) in list(graph_from_db.fetchall())]
    # Create the networkx graph with data from the db
    MainGraph.add_weighted_edges_from(edges_and_weights)

    print("-------------------------------------------------------------------------------")
    print("Creating graph...Done")
    print("Number of nodes in graph: ", MainGraph.number_of_nodes())
    print("Number of edges in graph: ", MainGraph.number_of_edges())
    print("-------------------------------------------------------------------------------")

    return MainGraph


def get_supgraph2(MainGraph, supgraph_size=1000):
    """"Choose the <supgraph_size> nodes with the highest out degrees
    as the subgraph"""""

    # calculate and sort the degrees of the users
    nodes_degree_list = [(node, degree) for node, degree in MainGraph.out_degree()]
    nodes_degree_list.sort(reverse=True, key=lambda x:x[1])

    # get the users with the supgraph_size highest degrees
    nodes_for_subgraph = np.array([nodes_and_degrees[0] for nodes_and_degrees in nodes_degree_list[:supgraph_size]])
    supgraph = MainGraph.subgraph(nodes_for_subgraph)

    # Stats of the supgraph
    print("-------------------------------------------------------------------------------")
    print("Supgraph stats:")
    print("Number of nodes in the supgraph: ", len(supgraph.nodes))
    print("Number of edges in the supgraph: ", len(supgraph.edges))

    return supgraph


def get_degree_distribution(graph):
    # Make the supgraph directed to calculate the
    # in and out degree
    directed_graph = nx.DiGraph()
    directed_graph.add_edges_from(graph.edges)

    # Calculate degrees of the supgraph
    degrees = [len(graph[node]) for node in graph.nodes]  # degrees of the nodes
    in_degrees = [directed_graph.in_degree(node) for node in directed_graph.nodes]  # in degrees
    out_degrees = [directed_graph.out_degree(node) for node in directed_graph.nodes]  # out degrees

    # Print results
    print("-------------------------------------------------------------------------------")
    print("Average degree: ", sum(degrees) / len(degrees))
    print("Maximum degree: ", max(degrees))
    print("-------------------------------------------------------------------------------")
    print("Average in degree: ", sum(in_degrees) / len(in_degrees))
    print("Maximum in degree: ", max(in_degrees))
    print("--------------------------------------------------------------------------------")
    print("Average out degree: ", sum(out_degrees) / len(out_degrees))
    print("Maximum out degree: ", max(out_degrees))
    print("-------------------------------------------------------------------------------")

    # Plot Results
    fig, (axis_1, axis_2, axis_3) = plt.subplots(1, 3, figsize=(15, 4))
    # fig.tight_layout()
    plot_pdf(degrees, axis_1, "degree distribution", xlabel="degree")
    plot_pdf(in_degrees, axis_2, "in degree distribution", xlabel="in degree")
    plot_pdf(out_degrees, axis_3, "out degree distribution", xlabel="out degree")


def get_reliable_sets_stats(ReliableSets):
    """Calculate some stats for the reliable sets"""
    lengths_of_Ris2 = []
    for RI in ReliableSets.values():
        lengths_of_Ris2.append(len(RI))

    print("Reliable sets stats:")
    print("----------------------------------------------------")
    print("Number of reliable sets: ", len(ReliableSets.values()))
    print("Average size: ", (sum(lengths_of_Ris2)) / len(lengths_of_Ris2))
    print("Max size", max(lengths_of_Ris2))
    print("Number of reliable sets of size>10: ", len([RS for RS in lengths_of_Ris2 if RS > 10]))
    print("Number of reliable sets of size>20: ", len([RS for RS in lengths_of_Ris2 if RS > 20]))
    print("Number of reliable sets of size>50: ", len([RS for RS in lengths_of_Ris2 if RS > 50]))
    print("----------------------------------------------------")


if __name__ == "__main__":
    SGdb = sqlite3.connect('C:/Users/gauss/Downloads/SG_2.db', uri=True)
    table_of_category = "SG_1"   # Table corresponding to the category we will create a graph for
    MainGraph = get_graph(table_of_category, SGdb)
    supgraph = get_supgraph2(MainGraph, supgraph_size=100)
    get_degree_distribution(supgraph)
    plt.show()
    SGdb.close()
