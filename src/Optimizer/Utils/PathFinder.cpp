#include <Optimizer/Utils/PathFinder.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>
#include <vector>

namespace NES {

PathFinderPtr PathFinder::create(NESTopologyPlanPtr nesTopologyPlan) {
    return std::make_shared<PathFinder>(PathFinder(nesTopologyPlan));
}

PathFinder::PathFinder(NESTopologyPlanPtr nesTopologyPlan) : nesTopologyPlan(nesTopologyPlan) {
    NES_DEBUG("PathFinder()");
}

std::vector<NESTopologyEntryPtr> PathFinder::findPathWithMaxBandwidth(NESTopologyEntryPtr source,
                                                                      NESTopologyEntryPtr destination) {

    NES_INFO("PathFinder: finding path wih maximum bandwidth between" << source->toString() << " and "
                                                                      << destination->toString());

    std::vector<std::vector<NESTopologyLinkPtr>> pathsWithLinks = findAllPathLinksBetween(source, destination);

    if (pathsWithLinks.empty()) {
        throw std::runtime_error(
            "Unable to find bath between " + source->toString() + " and " + destination->toString());
    }

    std::vector<NESTopologyLinkPtr> selectedPath;

    //If the number of paths are more than 1
    if (pathsWithLinks.size() > 1) {
        // We select the path whose minimum link capacity is maximum among the selected path
        size_t maxOfMinBandwidth = 0;
        size_t maxTotalBandwidth = 0;
        for (std::vector<NESTopologyLinkPtr> path : pathsWithLinks) {
            size_t minLinkBandwidth = UINT32_MAX;
            size_t totalBandwidth = 0;
            for (auto link : path) {
                if (minLinkBandwidth > link->getLinkCapacity()) {
                    totalBandwidth = totalBandwidth + link->getLinkCapacity();
                    minLinkBandwidth = link->getLinkCapacity();
                }
            }
            if ((maxOfMinBandwidth < minLinkBandwidth)
                || (maxOfMinBandwidth == minLinkBandwidth && totalBandwidth >= maxTotalBandwidth)) {

                maxTotalBandwidth = totalBandwidth;
                maxOfMinBandwidth = minLinkBandwidth;
                selectedPath = path;
            }
        }
    } else {
        selectedPath = pathsWithLinks[0];
    }

    return convertLinkPathIntoNodePath(source, selectedPath);
}

std::vector<NESTopologyEntryPtr> PathFinder::findPathWithMinLinkLatency(NESTopologyEntryPtr source,
                                                                        NESTopologyEntryPtr destination) {

    NES_INFO("PathFinder: finding path wih minimum link latency between" << source->toString() << " and "
                                                                         << destination->toString());

    std::vector<std::vector<NESTopologyLinkPtr>> pathsWithLinks = findAllPathLinksBetween(source, destination);

    if (pathsWithLinks.empty()) {
        throw std::runtime_error(
            "Unable to find bath between " + source->toString() + " and " + destination->toString());
    }

    std::vector<NESTopologyLinkPtr> selectedPath;

    //If the number of paths are more than 1
    if (pathsWithLinks.size() > 1) {
        // We select the path whose maximum link latency is minimum among the selected path
        size_t minOfMaxLatency = UINT32_MAX;
        size_t minTotalLatency = UINT32_MAX;
        for (std::vector<NESTopologyLinkPtr> path : pathsWithLinks) {
            size_t maxLinkLatency = 0;
            size_t totalLatency = 0;
            for (auto link : path) {
                totalLatency = totalLatency + link->getLinkLatency();
                if (link->getLinkLatency() > maxLinkLatency) {
                    maxLinkLatency = link->getLinkLatency();
                }
            }

            if (minOfMaxLatency > maxLinkLatency
                || (minOfMaxLatency == maxLinkLatency && minTotalLatency > totalLatency)) {

                minOfMaxLatency = maxLinkLatency;
                minTotalLatency = totalLatency;
                selectedPath = path;
            }
        }
    } else {
        selectedPath = pathsWithLinks[0];
    }

    return convertLinkPathIntoNodePath(source, selectedPath);
}

std::vector<NESTopologyEntryPtr> PathFinder::findPathBetween(NES::NESTopologyEntryPtr source,
                                                             NES::NESTopologyEntryPtr destination) {

    NES_INFO("PathFinder: finding a random path between " << source->toString() << " and "
                                                          << destination->toString());

    const std::vector<NESTopologyLinkPtr>& startLinks = nesTopologyPlan->getNESTopologyGraph()->getAllEdgesFromNode(source);

    if (startLinks.empty()) {
        throw std::runtime_error(
            "Source " + source->toString() + " not connected to any other node");
    }

    NESTopologyLinkPtr candidateLink = startLinks[0];

    std::vector<NESTopologyLinkPtr> traversedPath;

    while (candidateLink) {

        traversedPath.push_back(candidateLink);
        if (candidateLink->getDestNode()->getId() == destination->getId()) {
            break;
        } else {
            std::vector<NESTopologyLinkPtr>
                nextCandidatesLinks = nesTopologyPlan->getNESTopologyGraph()->getAllEdgesFromNode(candidateLink->getDestNode());
            if (nextCandidatesLinks.empty()) {
                return std::vector<NESTopologyEntryPtr>{};
            }
            candidateLink = nextCandidatesLinks[0];
        }
    }

    return convertLinkPathIntoNodePath(source, traversedPath);
}

std::map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>> PathFinder::findUniquePathBetween(std::vector<
                                                                                                      NESTopologyEntryPtr>
                                                                                                      sources,
                                                                                                  NESTopologyEntryPtr destination) {

    std::map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>> result;

    NES_INFO("PathFinder: finding unique path between set of sources and destination");

    if (sources.size() == 1) {
        NES_INFO("PathFinder: Only one source provided. Finding a path between source and destination.");
        result[sources[0]] = findPathBetween(sources[0], destination);
        return result;
    }

    std::map<NESTopologyEntryPtr, std::vector<std::vector<NESTopologyLinkPtr>>> sourceToPathsMap;

    NES_DEBUG("PathFinder: find all paths between set of sources and destination");
    for (NESTopologyEntryPtr source : sources) {

        std::vector<std::vector<NESTopologyLinkPtr>> allPathLinksBetween = findAllPathLinksBetween(source, destination);

        if (allPathLinksBetween.empty()) {
            throw std::runtime_error(
                "PathFinder: found no path between " + source->toString() + " and " + destination->toString());
        }
        NES_DEBUG("PathFinder: adding paths to sourceToPathsMap");
        sourceToPathsMap[source] = allPathLinksBetween;
    }

    bool needPruning = true;
    bool remainingSourceMapChanged = true;
    while (needPruning && remainingSourceMapChanged) {

        NES_DEBUG("PathFinder: resetting the flags to prune and changes in the map.");
        needPruning = false;
        remainingSourceMapChanged = false;

        NES_DEBUG("PathFinder: compare the paths of a source to the paths of rest of sources and identify the "
                  "path with maximum matching edges.");

        for (size_t i = 0; i < sources.size(); i++) {
            std::vector<std::vector<NESTopologyLinkPtr>> allPathsForSourceAt_i = sourceToPathsMap[sources[i]];

            //Stores the paths remaining after the pruning
            std::vector<std::vector<NESTopologyLinkPtr>> pathsAfterPruningForSourceAt_i;

            size_t maxCommonPathLinkForSource_i = 0;

            for (const std::vector<NESTopologyLinkPtr>& currentPathForSourceAt_i : allPathsForSourceAt_i) {

                size_t maxCommonPathLinkForCurrentPath = 0;

                for (size_t j = 0; j < sources.size(); j++) {

                    if (i == j) {
                        NES_DEBUG("PathFinder: skipping source paths comparision to themselves.");
                        continue;
                    }

                    std::vector<std::vector<NESTopologyLinkPtr>>
                        allPathsForSourceAt_j = sourceToPathsMap[sources[j]];

                    NES_DEBUG("PathFinder: get all paths for another source and perform comparision.");
                    for (std::vector<NESTopologyLinkPtr> currentPathForSourceAt_j : allPathsForSourceAt_j) {
                        for (NESTopologyLinkPtr link_i : currentPathForSourceAt_i) {
                            for (NESTopologyLinkPtr link_j : currentPathForSourceAt_j) {
                                if (link_i->getId() == link_j->getId()) {
                                    NES_DEBUG("PathFinder: if edge is similar increase the path weight.");
                                    maxCommonPathLinkForCurrentPath++;
                                }
                            }
                        }
                    }
                }

                if (maxCommonPathLinkForCurrentPath > maxCommonPathLinkForSource_i) {
                    NES_DEBUG("PathFinder: if the weight of maximum common path for the target source is less than "
                              "the current path considered of the target source then make it the maximum common path.");
                    maxCommonPathLinkForSource_i = maxCommonPathLinkForCurrentPath;
                    pathsAfterPruningForSourceAt_i.clear();
                    pathsAfterPruningForSourceAt_i.push_back(currentPathForSourceAt_i);
                } else if (maxCommonPathLinkForCurrentPath == maxCommonPathLinkForSource_i) {
                    NES_DEBUG("PathFinder: if the weight of maximum common path for the target source is equal to "
                              "the current path considered of the target source then add it to the list of potential common path.");
                    pathsAfterPruningForSourceAt_i.push_back(currentPathForSourceAt_i);
                }
            }

            //check if the path list pruned for the source
            if (sourceToPathsMap[sources[i]].size() != pathsAfterPruningForSourceAt_i.size()) {
                NES_DEBUG("PathFinder: if the sourceToPathsMap changed then make the remainingSourceMapChanged "
                          "flag true.");
                remainingSourceMapChanged = true;
            }
            sourceToPathsMap[sources[i]] = pathsAfterPruningForSourceAt_i;

            if (pathsAfterPruningForSourceAt_i.size() == 1) {
                result[sources[i]] =
                    convertLinkPathIntoNodePath(sources[i], pathsAfterPruningForSourceAt_i[0]);
            } else {
                NES_DEBUG("PathFinder: if for a source the number of paths after pruning is more than one "
                          "the we set the flag to prune further as true.");
                needPruning = true;
            }
        }
    }

    //FIXME : Still need to handle if pruning is required and the source to path mapping does not changes
    if (needPruning && !remainingSourceMapChanged) {
        NES_DEBUG("PathFinder: if for a source the number of paths after pruning is more than one "
                  "and there are no changes in the sourceToPathsMap then we have to add some circuit breaking logic here.");
        //TODO: write logic here
    }

    return result;
}

std::vector<std::vector<NESTopologyEntryPtr>> PathFinder::findAllPathsBetween(NESTopologyEntryPtr source,
                                                                              NESTopologyEntryPtr destination) {

    NES_INFO("PathFinder: finding all paths between source" << source->toString() << " and destination "
                                                            << destination->toString());

    const std::vector<std::vector<NESTopologyLinkPtr>>& allPaths = findAllPathLinksBetween(source, destination);

    std::vector<std::vector<NESTopologyEntryPtr>> result;

    for (auto path : allPaths) {
        result.push_back(convertLinkPathIntoNodePath(source, path));
    }

    return result;
}

std::vector<std::vector<NESTopologyLinkPtr>> PathFinder::findAllPathLinksBetween(NESTopologyEntryPtr source,
                                                                                 NESTopologyEntryPtr destination) {

    const std::vector<NESTopologyLinkPtr>& startLinks = nesTopologyPlan->getNESTopologyGraph()->getAllEdgesFromNode(source);

    std::deque<NESTopologyLinkPtr> candidateLinks;
    copy(startLinks.begin(), startLinks.end(), front_inserter(candidateLinks));

    std::vector<std::vector<NESTopologyLinkPtr>> setOfVisitedPaths;

    std::vector<NESTopologyLinkPtr> traversedPath;

    while (!candidateLinks.empty()) {

        NESTopologyLinkPtr link = candidateLinks.front();
        candidateLinks.pop_front();
        traversedPath.push_back(link);
        if (link->getDestNodeId() == destination->getId()) {
            setOfVisitedPaths.push_back(traversedPath);
            if (!candidateLinks.empty()) {
                traversedPath = backTrackTraversedPathTill(traversedPath, candidateLinks.front()->getSourceNode());
            }
        } else if (link->getDestNodeId() == nesTopologyPlan->getRootNode()->getId()) {
            if (!candidateLinks.empty()) {
                traversedPath = backTrackTraversedPathTill(traversedPath, candidateLinks.front()->getSourceNode());
            }
        } else {
            const std::vector<NESTopologyLinkPtr>& nextCandidateLinks = nesTopologyPlan->getNESTopologyGraph()->getAllEdgesFromNode(link->getDestNode());
            copy(nextCandidateLinks.begin(), nextCandidateLinks.end(), front_inserter(candidateLinks));
        }
    }

    return setOfVisitedPaths;
}

std::vector<NESTopologyEntryPtr> PathFinder::convertLinkPathIntoNodePath(const NESTopologyEntryPtr source,
                                                                    const std::vector<NESTopologyLinkPtr>& selectedPath) {

    NES_INFO("PathFinder: convert a path with ege information to a path containing vertex information");

    //adding the source node
    std::vector<NESTopologyEntryPtr> result = {source};

    //Build an array with nodes on the selected path
    for (NESTopologyLinkPtr link : selectedPath) {
        result.push_back(link->getDestNode());
    }
    return result;
}

std::vector<NESTopologyLinkPtr> PathFinder::backTrackTraversedPathTill(std::vector<NESTopologyLinkPtr> path,
                                                                       NES::NESTopologyEntryPtr nesNode) {
    auto itr = path.end();
    while (itr != path.begin()) {
        --itr;
        if (itr.operator*()->getDestNodeId() != nesNode->getId()) {
            path.erase(itr);
        } else {
            break;
        }
    }
    return path;
}

}// namespace NES
