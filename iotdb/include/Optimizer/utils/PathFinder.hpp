#ifndef NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_
#define NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_

#include <iostream>
#include <memory>
#include <vector>
#include <map>

namespace NES {

class NESTopologyEntry;
typedef std::shared_ptr<NESTopologyEntry> NESTopologyEntryPtr;

class NESTopologyLink;
typedef std::shared_ptr<NESTopologyLink> NESTopologyLinkPtr;

/**
 * @brief This class is used for finding the paths between any given nodes.
 */
class PathFinder {

  public:

    /**
     * @brief Find a set of paths between given set of sources and a common destination such that their are maximum intersection.
     *
     * Following is how the algorithm works:
     *
     * 1.) Find all paths for all source and destination pairs.
     * 2.) If the total returned paths for a source and destination pair is just one, then add the single path to the
     * result map for the corresponding source
     * 3.) For remaining list of source and destination pairs and corresponding paths we try to compute the most common path as follow:
     *      a.) We start with the first entry of the list and compare each path with all other paths of the remaining source destination pairs and add a weight of 1 for each matching edge.
     *      b.) For the pair under consideration, we select the path with maximum aggregated weight and add this path to the list.
     *      c.) We repeat the step a and b for all remaining pairs.
     *
     * @param sources : set of source nodes
     * @param destination : destination node
     * @return a map of source node to the vector of nodes in the path identified for the
     */
    std::map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>> findUniquePathBetween(std::vector<NESTopologyEntryPtr> sources,
                                                     NESTopologyEntryPtr destination);
    
    /**
     * @brief Find a path between given source and destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing nodes
     */
    std::vector<NESTopologyEntryPtr> findPathBetween(NESTopologyEntryPtr source,
                                                              NESTopologyEntryPtr destination);

    /**
     * @brief Find a path with maximum bandwidth between given source destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing nodes
     */
    std::vector<NESTopologyEntryPtr> findPathWithMaxBandwidth(NESTopologyEntryPtr source,
                                                              NESTopologyEntryPtr destination);

    /**
     * @brief Find a path with minimum link latency between given source destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing nodes
     */
    std::vector<NESTopologyEntryPtr> findPathWithMinLinkLatency(NESTopologyEntryPtr source,
                                                              NESTopologyEntryPtr destination);

    /**
     * @brief Find all paths between given source destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing vector of paths
     */
    std::vector<std::vector<NESTopologyEntryPtr>> findAllPathsBetween(NESTopologyEntryPtr source, NESTopologyEntryPtr destination);

  private:

    /**
     * @brief Backtrack the path to the point where nesNode was the destination
     * @param path : the path
     * @param nesNode : the nes node pointer
     * @return backtracked path
     */
    std::vector<NESTopologyLinkPtr> backTrackTraversedPathTill(std::vector<NESTopologyLinkPtr> path,
                                                               NESTopologyEntryPtr nesNode);

    /**
     * @brief Find all paths between given source destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing vector of paths
     */
    std::vector<std::vector<NESTopologyLinkPtr>> findAllPathLinksBetween(NESTopologyEntryPtr source, NESTopologyEntryPtr destination);

    /**
     * @brief convert the link path into the node path
     * @param source: the start node
     * @param selectedPath : the link path to be converted
     * @return vector of nodes in the link path
     */
    std::vector<NESTopologyEntryPtr> convertLinkPathIntoNodePath(const NESTopologyEntryPtr source,
                                                                 const std::vector<NESTopologyLinkPtr>& selectedPath);
};
}

#endif //NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_
