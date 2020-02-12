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
        size_t maxOfMinBandwidth = INT32_MAX;
        for (std::vector<NESTopologyLinkPtr> pathWithLinks : pathsWithLinks) {
            for (auto link : pathWithLinks) {
                if (maxOfMinBandwidth > link->getLinkCapacity()) {
                    maxOfMinBandwidth = link->getLinkLatency();
                    selectedPath = pathWithLinks;
                }
            }
        }
    } else {
        selectedPath = pathsWithLinks[0];
    }

    //adding the source node
    std::vector<NESTopologyEntryPtr> result = {source};

    //Build an array with nodes on the selected path
    for (NESTopologyLinkPtr link : selectedPath) {
        result.push_back(link->getDestNode());
    }

    return result;
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
        size_t minOfMaxLatency = 0;
        for (std::vector<NESTopologyLinkPtr> pathWithLinks : pathsWithLinks) {
            for (auto link : pathWithLinks) {
                if (minOfMaxLatency < link->getLinkCapacity()) {
                    minOfMaxLatency = link->getLinkLatency();
                    selectedPath = pathWithLinks;
                }
            }
        }
    } else {
        selectedPath = pathsWithLinks[0];
    }

    //adding the source node
    std::vector<NESTopologyEntryPtr> result = {source};

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

    NESTopologyLinkPtr candidateLink = startLinks[0];

    vector<NESTopologyLinkPtr> traversedPath = {};

    while (!candidateLink) {

        traversedPath.push_back(candidateLink);
        if (candidateLink->getDestNode()->getId() == destination->getId()) {
            break;
        } else {
            const vector<NESTopologyLinkPtr>
                & nextCandidatesLinks = nesGraphPtr->getAllEdgesFromNode(candidateLink->getDestNode());
            candidateLink = nextCandidatesLinks[0];
        }
    }

    //adding the source node
    std::vector<NESTopologyEntryPtr> result = {source};

    //Build an array with nodes on the selected path
    for (NESTopologyLinkPtr link : traversedPath) {
        result.push_back(link->getDestNode());
    }

    return result;
}

std::vector<NESTopologyEntryPtr> PathFinder::findUniquePathBetween(std::vector<NESTopologyEntryPtr> sources,
                                                                   NESTopologyEntryPtr destination) {

    for (NESTopologyEntryPtr source : sources) {
        vector<vector<NESTopologyLinkPtr>> allPathLinksBetween = findAllPathLinksBetween(source, destination);

        for (vector<NESTopologyLinkPtr> path : allPathLinksBetween) {
            std::string pathId = "";
            for (NESTopologyLinkPtr link: path) {
                pathId = pathId + "," + std::to_string(link->getId());
            }

        }

    }

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