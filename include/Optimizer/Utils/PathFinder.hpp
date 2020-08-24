#ifndef NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_
#define NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_

#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;

class PathFinder;
typedef std::shared_ptr<PathFinder> PathFinderPtr;

/**
 * @brief This class is used for finding the paths between any given nodes.
 */
class PathFinder {

  public:
    /**
     * @brief Find a set of paths between given set of sources and a common destination such that their are maximum
     * intersection.
     *
     * Following is how the algorithm works:
     *
     * 1.) Find all paths for all source and destination pairs.
     * 3.) Using the fetched information we prepare a sourceToPathsMap.
     * 4.) For each source in the sourceToPathsMap we try to compute the most common path for its list of paths as
     * follow:
     *      a.) We start with the first entry in the list and compare it with all other paths of the remaining source
     *      paths and add a weight of 1 for each matching edge.
     *      b.) For the source under consideration, we select the path with maximum aggregated weight and add this path
     *      to the new list of paths for the source.
     *      c.) If the new list of path for the source contains only one path then this path is added to the result map
     *      for corresponding source.
     *      d.) Else, we add the new path list to the sourceToPathsMap.
     * 5.) If the sourceToPathsMap has been changed but contains sources with path list with more than 1 path then we
     * repeat step 4.
     * 6.) We return pack the result map.
     *
     * @param sources : set of source nodes
     * @param destination : destination node
     * @return a map of source node to the vector of nodes in the path identified for the
     */
    std::map<TopologyNodePtr, std::vector<TopologyNodePtr>> findUniquePathBetween(std::vector<TopologyNodePtr> sources,
                                                                                  TopologyNodePtr destination);

    /**
     * @brief Find a path between given source and destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing nodes
     */
    std::vector<TopologyNodePtr> findPathBetween(TopologyNodePtr source, TopologyNodePtr destination);

    /**
     * @brief Find a path with maximum bandwidth between given source destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing nodes
     */
    std::vector<TopologyNodePtr> findPathWithMaxBandwidth(TopologyNodePtr source, TopologyNodePtr destination);

    /**
     * @brief Find a path with minimum link latency between given source destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing nodes
     */
    std::vector<TopologyNodePtr> findPathWithMinLinkLatency(TopologyNodePtr source, TopologyNodePtr destination);

    /**
     * @brief Find all paths between given source destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing vector of paths
     */
    std::vector<std::vector<TopologyNodePtr>> findAllPathsBetween(TopologyNodePtr source, TopologyNodePtr destination);

    static PathFinderPtr create(TopologyPtr topology);

  private:
    PathFinder(TopologyPtr topology);

    /**
     * @brief Backtrack the path to the point where nesNode was the destination
     * @param path : the path
     * @param nesNode : the nes node pointer
     * @return backtracked path
     */
    std::vector<TopologyNodePtr> backTrackTraversedPathTill(std::vector<TopologyNodePtr> path, TopologyNodePtr nesNode);

    /**
     * @brief Find all paths between given source destination
     * @param source : source node
     * @param destination : destination node
     * @return vector containing vector of paths
     */
    std::vector<std::vector<TopologyNodePtr>> findAllPathLinksBetween(TopologyNodePtr source, TopologyNodePtr destination);

    /**
     * @brief convert the link path into the node path
     * @param source: the start node
     * @param selectedPath : the link path to be converted
     * @return vector of nodes in the link path
     */
    std::vector<TopologyNodePtr> convertLinkPathIntoNodePath(const TopologyNodePtr source, const std::vector<TopologyNodePtr>& selectedPath);

    TopologyPtr topology;
};
}// namespace NES

#endif//NES_INCLUDE_OPTIMIZER_UTILS_PATHFINDER_HPP_
