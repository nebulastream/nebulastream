#ifndef NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_
#define NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_

#include <iostream>
#include <memory>
#include <vector>

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
     * @param sources : set of source nodes
     * @param destination : destination node
     * @return vector containing a vector of nodes in each of the identified path
     */
    std::vector<NESTopologyEntryPtr> findUniquePathBetween(std::vector<NESTopologyEntryPtr> sources,
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

};
}

#endif //NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_
