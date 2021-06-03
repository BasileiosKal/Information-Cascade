from utils import *
from MonteCarlo_utils import *

if __name__=="__main__":
    # ====================================================================================================== #
    # Create the graphs
    # ====================================================================================================== #
    # The local db with the graph
    SGdb = sqlite3.connect('C:/Users/gauss/Downloads/SG_2.db', uri=True)
    # Table corresponding to the category we will create a graph for
    table_of_category = "SG_1"
    # Create main graph
    MainGraph = get_graph(table_of_category, SGdb)
    # Get sup graph
    supgraph = get_supgraph2(MainGraph, supgraph_size=10000)
    # Get degree stats
    # get_degree_distribution(supgraph)
    # Close the db
    SGdb.close()


    # ====================================================================================================== #
    # Run Monte Carlo
    # ====================================================================================================== #
    ReliableSets = run_MonteCarlo(supgraph, runs=1000, threshold=0.02)


    # ====================================================================================================== #
    # Monte Carlo Results Stats
    # ====================================================================================================== #
    # Lengths of the Reliable Sets
    lengths_of_Ris = []
    for RI in ReliableSets.values():
        lengths_of_Ris.append(len(RI))

    print("Reliable sets stats:")
    print("----------------------------------------------------")
    print("Number of reliable sets: ", len(ReliableSets.values()))
    print("Average size: ", (sum(lengths_of_Ris))/len(lengths_of_Ris))
    print("Max size", max(lengths_of_Ris))
    print("Number of reliable sets of size>10: ", len([RS for RS in lengths_of_Ris if RS > 10]))
    print("Number of reliable sets of size>20: ", len([RS for RS in lengths_of_Ris if RS > 20]))
    print("Number of reliable sets of size>50: ", len([RS for RS in lengths_of_Ris if RS > 50]))
    print("----------------------------------------------------")

    # Sizes of the reachable sets
    RSMC_fig, RSMC_axis = plt.subplots()

    plot_pdf(lengths_of_Ris, RSMC_axis, "Reliable Set Size for Category 3", xlabel="Reliable Set Size")
