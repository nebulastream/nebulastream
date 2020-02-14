#include "Optimizer/utils/PathFinder.hpp"
#include <Topology/NESTopologyEntry.hpp>
#include <Topology/NESTopologyManager.hpp>

namespace NES {

std::vector<NESTopologyEntryPtr> PathFinder::findPathWithMaxBandwidth(NESTopologyEntryPtr source,
                                                                      NESTopologyEntryPtr destination) {

    std::vector<std::vector<NESTopologyLinkPtr>> pathsWithLinks = findAllPathLinksBetween(source, destination);

    if (pathsWithLinks.empty()) {
        throw std::runtime_error(
            "Unable to find bath between " + source->toString() + " and " + destination->toString());
    }

    std::vector<NESTopologyLinkPtr> selectedPath = {};

    //If the number of paths are more than 1
    if (pathsWithLinks.size() > 1) {
        // We select the path whose minimum link capacity is maximum among the selected path
        size_t maxOfMinBandwidth = 0;
        for (std::vector<NESTopologyLinkPtr> pathWithLinks : pathsWithLinks) {
            size_t minLinkBandwidth = INT32_MAX;
            for (auto link : pathWithLinks) {
                if (minLinkBandwidth > link->getLinkCapacity()) {
                    minLinkBandwidth = link->getLinkCapacity();
                }
            }
            if(maxOfMinBandwidth < minLinkBandwidth) {
                maxOfMinBandwidth = minLinkBandwidth;
                selectedPath = pathWithLinks;
            }
        }
    } else {
        selectedPath = pathsWithLinks[0];
    }

    return convertLinkPathIntoNodePath(source, selectedPath);
}

std::vector<NESTopologyEntryPtr> PathFinder::findPathWithMinLinkLatency(NESTopologyEntryPtr source,
                                                                        NESTopologyEntryPtr destination) {
    std::vector<std::vector<NESTopologyLinkPtr>> pathsWithLinks = findAllPathLinksBetween(source, destination);

    if (pathsWithLinks.empty()) {
        throw std::runtime_error(
            "Unable to find bath between " + source->toString() + " and " + destination->toString());
    }

    std::vector<NESTopologyLinkPtr> selectedPath = {};

    //If the number of paths are more than 1
    if (pathsWithLinks.size() > 1) {
        // We select the path whose maximum link latency is minimum among the selected path
        size_t minOfMaxLatency = INT32_MAX;
        for (std::vector<NESTopologyLinkPtr> pathWithLinks : pathsWithLinks) {
            size_t maxLinkLatency = 0;
            for (auto link : pathWithLinks) {
                if (link->getLinkCapacity() > maxLinkLatency) {
                    maxLinkLatency = link->getLinkLatency();
                }
            }

            if(minOfMaxLatency > maxLinkLatency) {
                minOfMaxLatency = maxLinkLatency;
                selectedPath = pathWithLinks;
            }
        }
    } else {
        selectedPath = pathsWithLinks[0];
    }

    return convertLinkPathIntoNodePath(source, selectedPath);
}

vector<NESTopologyEntryPtr> PathFinder::convertLinkPathIntoNodePath(const NESTopologyEntryPtr source,
                                                                    const vector<NESTopologyLinkPtr>& selectedPath) {

    //adding the source node
    vector<NESTopologyEntryPtr> result = {source};

    //Build an array with nodes on the selected path
    for (NESTopologyLinkPtr link : selectedPath) {
        result.push_back(link->getDestNode());
    }
    return result;
}

std::vector<NESTopologyEntryPtr> PathFinder::findPathBetween(NES::NESTopologyEntryPtr source,
                                                             NES::NESTopologyEntryPtr destination) {
    NESTopologyManager& topologyManager = NESTopologyManager::getInstance();

    const NESTopologyGraphPtr& nesGraphPtr = topologyManager.getNESTopologyPlan()->getNESTopologyGraph();
    const vector<NESTopologyLinkPtr>& startLinks = nesGraphPtr->getAllEdgesFromNode(source);

    if (startLinks.empty()) {
        throw std::runtime_error(
            "Source " + source->toString() + " not connected to any other node");
    }

    NESTopologyLinkPtr candidateLink = startLinks[0];

    vector<NESTopologyLinkPtr> traversedPath = {};

    while (candidateLink) {

        traversedPath.push_back(candidateLink);
        if (candidateLink->getDestNode()->getId() == destination->getId()) {
            break;
        } else {
            vector<NESTopologyLinkPtr>
                nextCandidatesLinks = nesGraphPtr->getAllEdgesFromNode(candidateLink->getDestNode());
            if (nextCandidatesLinks.empty()) {
                return std::vector<NESTopologyEntryPtr>{};
            }
            candidateLink = nextCandidatesLinks[0];
        }
    }

    return convertLinkPathIntoNodePath(source, traversedPath);
}

std::map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>> PathFinder::findUniquePathBetween(std::vector<
    NESTopologyEntryPtr> sources, NESTopologyEntryPtr destination) {

    std::map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>> result;

    std::map<NESTopologyEntryPtr, std::vector<std::vector<NESTopologyLinkPtr>>> sourceToPathsMap;
    for (NESTopologyEntryPtr source : sources) {
        std::vector<std::vector<NESTopologyLinkPtr>> allPathLinksBetween = findAllPathLinksBetween(source, destination);

        if (allPathLinksBetween.empty()) {
            throw std::runtime_error(
                "PathFinder: found no path between " + source->toString() + " and " + destination->toString());
        }

        if (allPathLinksBetween.size() == 1) {
            // if there is only one path between source and destination then select the path as the final path and store
            // in the result
            result[source] = convertLinkPathIntoNodePath(source, allPathLinksBetween[0]);
        } else {
            // store all paths between source and destination for later processing
            sourceToPathsMap[source] = allPathLinksBetween;
        }
    }

    vector<NESTopologyEntryPtr> remainingSources;
    for (const auto& pair : sourceToPathsMap) {
        remainingSources.push_back(pair.first);
    }

    bool needPruning = true;
    bool remainingSourceMapChanged = true;
    while (needPruning && remainingSourceMapChanged) {
        needPruning = false;
        remainingSourceMapChanged = false;

        for (size_t i = 0; i < remainingSources.size(); i++) {
            std::vector<std::vector<NESTopologyLinkPtr>> allPathsForSourceAt_i = sourceToPathsMap[remainingSources[i]];

            //Stores the paths remaining after the pruning
            std::vector<std::vector<NESTopologyLinkPtr>> pathsAfterPruningForSourceAt_i;

            size_t maxCommonPathLinkForSource_i = 0;

            for (const std::vector<NESTopologyLinkPtr>& currentPathForSourceAt_i : allPathsForSourceAt_i) {

                size_t maxCommonPathLinkForCurrentPath = 0;

                for (size_t j = 0; j < remainingSources.size(); j++) {
                    if (i == j) {
                        continue;
                    }
                    std::vector<std::vector<NESTopologyLinkPtr>>
                        allPathsForSourceAt_j = sourceToPathsMap[remainingSources[j]];

                    for (std::vector<NESTopologyLinkPtr> currentPathForSourceAt_j : allPathsForSourceAt_j) {
                        for (NESTopologyLinkPtr link_i : currentPathForSourceAt_i) {
                            for (NESTopologyLinkPtr link_j : currentPathForSourceAt_j) {
                                if (link_i->getId() == link_j->getId()) {
                                    maxCommonPathLinkForCurrentPath++;
                                }
                            }
                        }
                    }
                }

                if (maxCommonPathLinkForCurrentPath > maxCommonPathLinkForSource_i) {
                    maxCommonPathLinkForSource_i = maxCommonPathLinkForCurrentPath;
                    pathsAfterPruningForSourceAt_i.clear();
                    pathsAfterPruningForSourceAt_i.push_back(currentPathForSourceAt_i);
                } else if (maxCommonPathLinkForCurrentPath == maxCommonPathLinkForSource_i) {
                    pathsAfterPruningForSourceAt_i.push_back(currentPathForSourceAt_i);
                }
            }

            //check if the path list pruned for the source
            if (sourceToPathsMap[remainingSources[i]].size() != pathsAfterPruningForSourceAt_i.size()) {
                remainingSourceMapChanged = true;
            }
            sourceToPathsMap[remainingSources[i]] = pathsAfterPruningForSourceAt_i;

            if (pathsAfterPruningForSourceAt_i.size() == 1) {
                result[remainingSources[i]] =
                    convertLinkPathIntoNodePath(remainingSources[i], pathsAfterPruningForSourceAt_i[0]);
            } else {
                needPruning = true;
            }
        }
    }

    //FIXME : Still need to handle if pruning is required and the source to path mapping does not changes
    if (needPruning && !remainingSourceMapChanged) {
        //TODO: write logic here
    }

    return result;
}
std::vector<std::vector<NESTopologyEntryPtr>> PathFinder::findAllPathsBetween(NESTopologyEntryPtr source,
                                                                              NESTopologyEntryPtr destination) {

    const vector<std::vector<NESTopologyLinkPtr>>& allPaths = findAllPathLinksBetween(source, destination);

    std::vector<std::vector<NESTopologyEntryPtr>> result;

    for(auto path : allPaths){
        result.push_back(convertLinkPathIntoNodePath(source, path));
    }

    return result;
}

std::vector<std::vector<NESTopologyLinkPtr>> PathFinder::findAllPathLinksBetween(NESTopologyEntryPtr source,
                                                                                 NESTopologyEntryPtr destination) {

    NESTopologyManager& topologyManager = NESTopologyManager::getInstance();

    const NESTopologyGraphPtr& nesGraphPtr = topologyManager.getNESTopologyPlan()->getNESTopologyGraph();
    const vector<NESTopologyLinkPtr>& startLinks = nesGraphPtr->getAllEdgesFromNode(source);

    deque<NESTopologyLinkPtr> candidateLinks = {};
    copy(startLinks.begin(), startLinks.end(), front_inserter(candidateLinks));

    vector<vector<NESTopologyLinkPtr>> setOfVisitedPaths = {};

    vector<NESTopologyLinkPtr> traversedPath = {};

    while (!candidateLinks.empty()) {

        NESTopologyLinkPtr link = candidateLinks.front();
        candidateLinks.pop_front();
        traversedPath.push_back(link);
        if (link->getDestNode()->getId() == destination->getId()) {
            setOfVisitedPaths.push_back(traversedPath);
            if (!candidateLinks.empty()) {
                traversedPath = backTrackTraversedPathTill(traversedPath, candidateLinks.front()->getSourceNode());
            }
        } else {
            const vector<NESTopologyLinkPtr>
                & nextCandidateLinks = nesGraphPtr->getAllEdgesFromNode(link->getDestNode());
            copy(nextCandidateLinks.begin(), nextCandidateLinks.end(), front_inserter(candidateLinks));
        }
    }

    return setOfVisitedPaths;
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


}