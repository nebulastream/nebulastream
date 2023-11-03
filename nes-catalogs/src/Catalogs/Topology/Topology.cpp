/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/Topology/TopologyNodeSet.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <algorithm>
#include <deque>
#include <utility>

namespace NES {

Topology::Topology() : rootNode(nullptr) {}

TopologyPtr Topology::create() { return std::shared_ptr<Topology>(new Topology()); }

bool Topology::addNewTopologyNodeAsChild(const TopologyNodePtr& parent, const TopologyNodePtr& newNode) {
    std::unique_lock lock(topologyLock);
    uint64_t newNodeId = newNode->getId();
    if (indexOnNodeIds.find(newNodeId) == indexOnNodeIds.end()) {
        NES_INFO("Topology: Adding New Node {} to the catalog of nodes.", newNode->toString());
        indexOnNodeIds[newNodeId] = newNode;
    }
    NES_INFO("Topology: Adding Node {} as child to the node {}", newNode->toString(), parent->toString());
    return parent->addChild(newNode);
}

bool Topology::removePhysicalNode(const TopologyNodePtr& nodeToRemove) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Removing Node {}", nodeToRemove->toString());

    uint64_t idOfNodeToRemove = nodeToRemove->getId();
    if (indexOnNodeIds.find(idOfNodeToRemove) == indexOnNodeIds.end()) {
        NES_WARNING("Topology: The physical node {} doesn't exists in the system.", idOfNodeToRemove);
        return true;
    }

    if (!rootNode) {
        NES_WARNING("Topology: No root node exists in the topology");
        return false;
    }

    if (rootNode->getId() == idOfNodeToRemove) {
        NES_WARNING("Topology: Attempt to remove the root node. Removing root node is not allowed.");
        return false;
    }

    nodeToRemove->removeAllParent();
    nodeToRemove->removeChildren();
    indexOnNodeIds.erase(idOfNodeToRemove);
    NES_DEBUG("Topology: Successfully removed the node.");
    return true;
}

std::vector<TopologyNodePtr> Topology::findPathBetween(const std::vector<TopologyNodePtr>& sourceNodes,
                                                       const std::vector<TopologyNodePtr>& destinationNodes) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding path between set of start and destination nodes");
    std::vector<TopologyNodePtr> startNodesOfGraph;
    for (const auto& sourceNode : sourceNodes) {
        NES_TRACE("Topology: Finding all paths between the source node {} and a set of destination nodes",
                  sourceNode->toString());
        std::map<uint64_t, TopologyNodePtr> mapOfUniqueNodes;
        TopologyNodePtr startNodeOfGraph = find(sourceNode, destinationNodes, mapOfUniqueNodes);
        NES_TRACE("Topology: Validate if all destination nodes reachable");
        for (const auto& destinationNode : destinationNodes) {
            if (mapOfUniqueNodes.find(destinationNode->getId()) == mapOfUniqueNodes.end()) {
                NES_ERROR("Topology: Unable to find path between source node {} and destination node{}",
                          sourceNode->toString(),
                          destinationNode->toString());
                return {};
            }
        }
        NES_TRACE("Topology: Push the start node of the graph into a collection of start nodes");
        startNodesOfGraph.push_back(startNodeOfGraph);
    }
    NES_TRACE("Topology: Merge all found sub-graphs together to create a single sub graph and return the set of start nodes of "
              "the merged graph.");
    return mergeSubGraphs(startNodesOfGraph);
}

std::vector<TopologyNodePtr> Topology::mergeSubGraphs(const std::vector<TopologyNodePtr>& startNodes) {
    NES_INFO("Topology: Merge {} sub-graphs to create a single sub-graph", startNodes.size());

    NES_DEBUG("Topology: Compute a map storing number of times a node occurred in different sub-graphs");
    std::map<uint64_t, uint32_t> nodeCountMap;
    for (const auto& startNode : startNodes) {
        NES_TRACE("Topology: Fetch all ancestor nodes of the given start node");
        const std::vector<NodePtr> family = startNode->getAndFlattenAllAncestors();
        NES_TRACE(
            "Topology: Iterate over the family members and add the information in the node count map about the node occurrence");
        for (const auto& member : family) {
            uint64_t nodeId = member->as<TopologyNode>()->getId();
            if (nodeCountMap.find(nodeId) != nodeCountMap.end()) {
                NES_TRACE("Topology: Family member already present increment the occurrence count");
                uint32_t count = nodeCountMap[nodeId];
                nodeCountMap[nodeId] = count + 1;
            } else {
                NES_TRACE("Topology: Add family member into the node count map");
                nodeCountMap[nodeId] = 1;
            }
        }
    }

    NES_DEBUG("Topology: Iterate over each sub-graph and compute a single merged sub-graph");
    std::vector<TopologyNodePtr> result;
    std::map<uint64_t, TopologyNodePtr> mergedGraphNodeMap;
    for (const auto& startNode : startNodes) {
        NES_DEBUG(
            "Topology: Check if the node already present in the new merged graph and add a copy of the node if not present");
        if (mergedGraphNodeMap.find(startNode->getId()) == mergedGraphNodeMap.end()) {
            TopologyNodePtr copyOfStartNode = startNode->copy();
            NES_DEBUG("Topology: Add the start node to the list of start nodes for the new merged graph");
            result.push_back(copyOfStartNode);
            mergedGraphNodeMap[startNode->getId()] = copyOfStartNode;
        }
        NES_DEBUG("Topology: Iterate over the ancestry of the start node and add the eligible nodes to new merged graph");
        TopologyNodePtr childNode = startNode;
        while (childNode) {
            NES_TRACE("Topology: Get all parents of the child node to select the next parent to traverse.");
            std::vector<NodePtr> parents = childNode->getParents();
            TopologyNodePtr selectedParent;
            if (parents.size() > 1) {
                NES_TRACE("Topology: Found more than one parent for the node");
                NES_TRACE("Topology: Iterate over all parents and select the parent node that has the max cost value.");
                double maxCost = 0;
                for (auto& parent : parents) {

                    NES_TRACE("Topology: Get all ancestor of the node and aggregate their occurrence counts.");
                    std::vector<NodePtr> family = parent->getAndFlattenAllAncestors();
                    double occurrenceCount = 0;
                    for (auto& member : family) {
                        occurrenceCount = occurrenceCount + nodeCountMap[member->as<TopologyNode>()->getId()];
                    }

                    NES_TRACE("Topology: Compute cost by multiplying aggregate occurrence count with base multiplier and "
                              "dividing the result by the number of nodes in the path.");
                    double cost = (occurrenceCount * BASE_MULTIPLIER) / family.size();

                    if (cost > maxCost) {
                        NES_TRACE("Topology: The cost is more than max cost found till now.");
                        if (selectedParent) {
                            NES_TRACE("Topology: Remove the previously selected parent as parent to the current child node.");
                            childNode->removeParent(selectedParent);
                        }
                        maxCost = cost;
                        NES_TRACE("Topology: Mark this parent as next selected parent.");
                        selectedParent = parent->as<TopologyNode>();
                    } else {
                        NES_TRACE("Topology: The cost is less than max cost found till now.");
                        if (selectedParent) {
                            NES_TRACE("Topology: Remove this parent as parent to the current child node.");
                            childNode->removeParent(parent);
                        }
                    }
                }
            } else if (parents.size() == 1) {
                NES_TRACE("Topology: Found only one parent for the current child node");
                NES_TRACE("Topology: Set the parent as next parent to traverse");
                selectedParent = parents[0]->as<TopologyNode>();
            }

            if (selectedParent) {
                NES_TRACE("Topology: Found a new next parent to traverse");
                if (mergedGraphNodeMap.find(selectedParent->getId()) != mergedGraphNodeMap.end()) {
                    NES_TRACE("Topology: New next parent is already present in the new merged graph.");
                    TopologyNodePtr equivalentParentNode = mergedGraphNodeMap[selectedParent->getId()];
                    TopologyNodePtr equivalentChildNode = mergedGraphNodeMap[childNode->getId()];
                    NES_TRACE("Topology: Add the existing node, with id same as new next parent, as parent to the existing node "
                              "with id same as current child node");
                    equivalentChildNode->addParent(equivalentParentNode);
                } else {
                    NES_TRACE("Topology: New next parent is not present in the new merged graph.");
                    NES_TRACE(
                        "Topology: Add copy of new next parent as parent to the existing child node in the new merged graph.");
                    TopologyNodePtr copyOfSelectedParent = selectedParent->copy();
                    TopologyNodePtr equivalentChildNode = mergedGraphNodeMap[childNode->getId()];
                    equivalentChildNode->addParent(copyOfSelectedParent);
                    mergedGraphNodeMap[selectedParent->getId()] = copyOfSelectedParent;
                }
            }
            NES_TRACE("Topology: Assign new selected parent as next child node to traverse.");
            childNode = selectedParent;
        }
    }

    return result;
}

std::optional<TopologyNodePtr> Topology::findAllPathBetween(const TopologyNodePtr& startNode,
                                                            const TopologyNodePtr& destinationNode) {
    std::unique_lock lock(topologyLock);
    NES_DEBUG("Topology: Finding path between {} and {}", startNode->toString(), destinationNode->toString());

    std::optional<TopologyNodePtr> result;
    std::vector<TopologyNodePtr> searchedNodes{destinationNode};
    std::map<uint64_t, TopologyNodePtr> mapOfUniqueNodes;
    TopologyNodePtr found = find(startNode, searchedNodes, mapOfUniqueNodes);
    if (found) {
        NES_DEBUG("Topology: Found path between {} and {}", startNode->toString(), destinationNode->toString());
        return found;
    }
    NES_WARNING("Topology: Unable to find path between {} and {}", startNode->toString(), destinationNode->toString());
    return result;
}

TopologyNodePtr Topology::find(TopologyNodePtr testNode,
                               std::vector<TopologyNodePtr> searchedNodes,
                               std::map<uint64_t, TopologyNodePtr>& uniqueNodes) {

    NES_TRACE("Topology: check if test node is one of the searched node");
    auto found = std::find_if(searchedNodes.begin(), searchedNodes.end(), [&](const TopologyNodePtr& searchedNode) {
        return searchedNode->getId() == testNode->getId();
    });

    if (found != searchedNodes.end()) {
        NES_DEBUG("Topology: found the destination node");
        if (uniqueNodes.find(testNode->getId()) == uniqueNodes.end()) {
            NES_TRACE("Topology: Insert the information about the test node in the unique node map");
            const TopologyNodePtr copyOfTestNode = testNode->copy();
            uniqueNodes[testNode->getId()] = copyOfTestNode;
        }
        NES_TRACE("Topology: Insert the information about the test node in the unique node map");
        return uniqueNodes[testNode->getId()];
    }

    std::vector<NodePtr> parents = testNode->getParents();
    std::vector<NodePtr> updatedParents;
    //filters out all parents that are marked for maintenance, as these should be ignored during path finding
    for (auto& parent : parents) {
        if (!parent->as<TopologyNode>()->isUnderMaintenance()) {
            updatedParents.push_back(parent);
        }
    }

    if (updatedParents.empty()) {
        NES_WARNING("Topology: reached end of the tree but destination node not found.");
        return nullptr;
    }

    TopologyNodePtr foundNode = nullptr;
    for (auto& parent : updatedParents) {
        TopologyNodePtr foundInParent = find(parent->as<TopologyNode>(), searchedNodes, uniqueNodes);
        if (foundInParent) {
            NES_TRACE("Topology: found the destination node as the parent of the physical node.");
            if (!foundNode) {
                //TODO: SZ I don't understand how we can end up here
                if (uniqueNodes.find(testNode->getId()) == uniqueNodes.end()) {
                    const TopologyNodePtr copyOfTestNode = testNode->copy();
                    uniqueNodes[testNode->getId()] = copyOfTestNode;
                }
                foundNode = uniqueNodes[testNode->getId()];
            }
            NES_TRACE("Topology: Adding found node as parent to the copy of testNode.");
            foundNode->addParent(foundInParent);
        }
    }
    return foundNode;
}

std::string Topology::toString() {
    std::unique_lock lock(topologyLock);

    if (!rootNode) {
        NES_WARNING("Topology: No root node found");
        return "";
    }

    std::stringstream topologyInfo;
    topologyInfo << std::endl;

    // store pair of TopologyNodePtr and its depth in when printed
    std::deque<std::pair<TopologyNodePtr, uint64_t>> parentToPrint{std::make_pair(rootNode, 0)};

    // indent offset
    int indent = 2;

    // perform dfs traverse
    while (!parentToPrint.empty()) {
        std::pair<TopologyNodePtr, uint64_t> nodeToPrint = parentToPrint.front();
        parentToPrint.pop_front();
        for (std::size_t i = 0; i < indent * nodeToPrint.second; i++) {
            if (i % indent == 0) {
                topologyInfo << '|';
            } else {
                if (i >= indent * nodeToPrint.second - 1) {
                    topologyInfo << std::string(indent, '-');
                } else {
                    topologyInfo << std::string(indent, ' ');
                }
            }
        }
        topologyInfo << nodeToPrint.first->toString() << std::endl;

        for (const auto& child : nodeToPrint.first->getChildren()) {
            parentToPrint.emplace_front(child->as<TopologyNode>(), nodeToPrint.second + 1);
        }
    }
    return topologyInfo.str();
}

void Topology::print() { NES_DEBUG("Topology print:{}", toString()); }

bool Topology::nodeWithWorkerIdExists(TopologyNodeId workerId) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding if a physical node with worker id {} exists.", workerId);
    if (!rootNode) {
        NES_WARNING("Topology: Root node not found.");
        return false;
    }
    NES_TRACE("Topology: Traversing the topology using BFS.");
    BreadthFirstNodeIterator bfsIterator(rootNode);
    for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
        auto physicalNode = (*itr)->as<TopologyNode>();
        if (physicalNode->getId() == workerId) {
            NES_TRACE("Topology: Found a physical node {} with worker id {}", physicalNode->toString(), workerId);
            return true;
        }
    }
    return false;
}

TopologyNodePtr Topology::getRoot() {
    std::unique_lock lock(topologyLock);
    return rootNode;
}

TopologyNodePtr Topology::findNodeWithId(uint64_t nodeId) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Finding a physical node with id {}", nodeId);
    if (indexOnNodeIds.find(nodeId) != indexOnNodeIds.end()) {
        NES_DEBUG("Topology: Found a physical node with id {}", nodeId);
        return indexOnNodeIds[nodeId];
    }
    NES_WARNING("Topology: Unable to find a physical node with id {}", nodeId);
    return nullptr;
}

void Topology::setAsRoot(const TopologyNodePtr& physicalNode) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Setting physical node {} as root to the topology.", physicalNode->toString());
    indexOnNodeIds[physicalNode->getId()] = physicalNode;
    rootNode = physicalNode;
}

bool Topology::removeNodeAsChild(const TopologyNodePtr& parentNode, const TopologyNodePtr& childNode) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Removing node {} as child to the node {}", childNode->toString(), parentNode->toString());
    return parentNode->remove(childNode);
}

bool Topology::reduceResources(uint64_t nodeId, uint16_t amountToReduce) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Reduce {} resources from node with id {}", amountToReduce, nodeId);
    if (indexOnNodeIds.find(nodeId) == indexOnNodeIds.end()) {
        NES_WARNING("Topology: Unable to find node with id {}", nodeId);
        return false;
    }
    indexOnNodeIds[nodeId]->reduceResources(amountToReduce);
    return true;
}

bool Topology::increaseResources(uint64_t nodeId, uint16_t amountToIncrease) {
    std::unique_lock lock(topologyLock);
    NES_INFO("Topology: Increase {} resources from node with id {}", amountToIncrease, nodeId);
    if (indexOnNodeIds.find(nodeId) == indexOnNodeIds.end()) {
        NES_WARNING("Topology: Unable to find node with id {}", nodeId);
        return false;
    }
    indexOnNodeIds[nodeId]->increaseResources(amountToIncrease);
    return true;
}

template<typename T>
NES::TopologyNodeSet findIntersectionInner(T vecs);

template<>
NES::TopologyNodeSet findIntersectionInner(NES::TopologyNodeSet vecs) {
    return vecs;
}

template<>
NES::TopologyNodeSet findIntersectionInner(std::vector<NES::NodePtr> vecs) {
    NES::TopologyNodeSet result;
    std::transform(vecs.begin(), vecs.end(), std::inserter(result, result.begin()), [](const auto& node) {
        return node->template as<TopologyNode>();
    });
    return result;
}

template<>
NES::TopologyNodeSet findIntersectionInner(std::vector<NES::TopologyNodeSet> vecs) {
    if (vecs.empty()) {
        return {};
    }

    auto current = vecs[0];
    for (size_t i = 1; i < vecs.size(); ++i) {
        NES::TopologyNodeSet intersection;
        std::set_intersection(current.begin(),
                              current.end(),
                              vecs[i].begin(),
                              vecs[i].end(),
                              std::inserter(intersection, intersection.begin()),
                              NES::TopologyNodeCompare());
        current = std::move(intersection);
    }

    return current;
}

template<typename... VecOfVecs>
NES::TopologyNodeSet intersection(VecOfVecs&&... vecs) {
    return findIntersectionInner(std::vector<NES::TopologyNodeSet>{
        findIntersectionInner(std::forward<VecOfVecs>(vecs))...,
    });
}

static NES::TopologyNodeSet leafNodes(const TopologyNodeSet& nodes, Direction direction) {
    TopologyNodeSet result;
    std::copy_if(nodes.begin(), nodes.end(), std::inserter(result, result.begin()), [direction, &nodes](auto node) {
        auto adjacent = direction == Upstream ? node->getParents() : node->getChildren();
        std::sort(adjacent.begin(), adjacent.end(), [](auto a, auto b) {
            return a->template as<TopologyNode>()->getId() < b->template as<TopologyNode>()->getId();
        });
        return intersection(nodes, adjacent).empty();
    });
    NES_DEBUG("Found {} leaf nodes", result.size());
    return result;
}

NES::TopologyNodeSet Topology::findCommonChildren(std::vector<TopologyNodePtr> topologyNodes) {
    NES_INFO("Topology: find common child node for a set of parent topology nodes.");

    std::vector<TopologyNodeSet> allChildren;
    std::transform(topologyNodes.begin(), topologyNodes.end(), std::back_inserter(allChildren), [](auto node) {
        return node->dfs(Upstream);
    });
    return leafNodes(intersection(allChildren), Upstream);
}

NES::TopologyNodeSet Topology::findCommonAncestors(std::vector<TopologyNodePtr> topologyNodes) {
    NES_INFO("Topology: find common ancestor node for a set of child topology nodes.");

    std::vector<TopologyNodeSet> allAncestors;
    std::transform(topologyNodes.begin(), topologyNodes.end(), std::back_inserter(allAncestors), [](auto node) {
        return node->dfs(Downstream);
    });

    return leafNodes(intersection(allAncestors), Downstream);
}

NES::TopologyNodeSet Topology::findCommonNodesBetween(std::vector<TopologyNodePtr> childNodes,
                                                      std::vector<TopologyNodePtr> parenNodes) {
    NES_ASSERT2_FMT(!childNodes.empty(), "Calling findCommonNodeBetween with empty children");
    NES_ASSERT2_FMT(!parenNodes.empty(), "Calling findCommonNodeBetween with empty parents");

    std::vector<TopologyNodeSet> allChildren(childNodes.size());
    std::transform(childNodes.begin(), childNodes.end(), allChildren.begin(), [](const auto& node) {
        return node->dfs(Downstream);
    });
    std::vector<TopologyNodeSet> allAncestor(parenNodes.size());
    std::transform(parenNodes.begin(), parenNodes.end(), allAncestor.begin(), [](const auto& node) {
        return node->dfs(Upstream);
    });
    auto node_intersection = intersection(allChildren, allAncestor);

    return node_intersection;
}

NES::TopologyNodeSet Topology::findNodesBetween(const TopologyNodePtr& sourceNode, const TopologyNodePtr& destinationNode) {
    NES_DEBUG("Finding Nodes between Source: {} and Destination: {}", *sourceNode, *destinationNode);
    auto result = intersection(sourceNode->dfs(Downstream), destinationNode->dfs(Upstream));
    return result;
}

std::vector<TopologyNodePtr> Topology::sort(const NES::TopologyNodeSet& nodes, Direction direction) {
    std::vector<TopologyNodePtr> result;
    result.reserve(nodes.size());
    TopologyNodeSet set;
    std::copy(nodes.begin(), nodes.end(), std::inserter(set, set.begin()));
    auto adjacent = [direction](TopologyNodePtr node) {
        return direction == Downstream ? node->getChildren() : node->getParents();
    };

    while (!set.empty()) {
        TopologyNodeSet newSet;
        for (auto& node : set) {
            if (intersection(adjacent(node), set).empty()) {
                result.emplace_back(std::move(node));
            } else {
                newSet.insert(std::move(node));
            }
        }
        set = std::move(newSet);
    }

    return result;
}

NES::TopologyNodeSet Topology::findNodesBetween(std::vector<TopologyNodePtr> sourceNodes,
                                                std::vector<TopologyNodePtr> destinationNodes) {
    std::vector<TopologyNodeSet> allChildren(destinationNodes.size());
    std::vector<TopologyNodeSet> allParents(sourceNodes.size());
    std::transform(sourceNodes.begin(), sourceNodes.end(), allParents.begin(), [](auto node) {
        return node->dfs(Downstream);
    });
    std::transform(destinationNodes.begin(), destinationNodes.end(), allChildren.begin(), [](auto node) {
        return node->dfs(Upstream);
    });

    return intersection(intersection(allChildren), intersection(allParents));
}

TopologyNodePtr Topology::findTopologyNodeInSubgraphById(uint64_t id, const std::vector<TopologyNodePtr>& sourceNodes) {
    //First look in all source nodes
    for (const auto& sourceNode : sourceNodes) {
        if (sourceNode->getId() == id) {
            return sourceNode;
        }
    }

    //Perform DFS from the root node to find TopologyNode with matching identifier
    auto rootNodes = sourceNodes[0]->getAllRootNodes();
    //TODO: When we support more than 1 sink location please change the logic
    if (rootNodes.size() > 1) {
        NES_NOT_IMPLEMENTED();
    }
    auto topologyIterator = NES::DepthFirstNodeIterator(rootNodes[0]).begin();
    while (topologyIterator != NES::DepthFirstNodeIterator::end()) {
        auto currentTopologyNode = (*topologyIterator)->as<TopologyNode>();
        if (currentTopologyNode->getId() == id) {
            return currentTopologyNode;
        }
        ++topologyIterator;
    }
    return nullptr;
}
}// namespace NES