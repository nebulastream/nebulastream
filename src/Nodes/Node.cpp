#include <Nodes/Node.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <queue>
#include <unordered_set>

namespace NES {

Node::Node() : visited(false), recStack(false) {
}

Node::~Node() {
}

bool Node::addChildWithEqual(const NodePtr newNode) {
    if (newNode.get() == this) {
        NES_DEBUG("Node: Adding node to its self so skip add child with equal operation.");
        return false;
    }
    // add the node to the children
    children.push_back(newNode);

    // add the current node as a parents to the newNode
    newNode->parents.push_back(shared_from_this());
    return true;
}

bool Node::addChild(const NodePtr newNode) {
    if (newNode.get() == this) {
        NES_DEBUG("Node: Adding node to its self so will skip add child operation.");
        return false;
    }
    // checks if current new node is not part of children
    if (vectorContainsTheNode(children, newNode)) {
        NES_DEBUG("Node: the node is already part of its children so skip add chld operation.");
        return false;
    }
    // add the node to the children
    children.push_back(newNode);

    // add the current node as a parents to the newNode
    if (!vectorContainsTheNode(newNode->parents, shared_from_this())) {
        newNode->parents.push_back(shared_from_this());
    }
    return true;
}

bool Node::removeChild(const NodePtr node) {
    // check all children.
    for (auto nodeItr = children.begin(); nodeItr != children.end(); ++nodeItr) {
        NES_DEBUG("Node: remove " << (*nodeItr)->toString() << " from " << node->toString() << " this=" << this << " other=" << node.get());
        if ((*nodeItr)->equal(node)) {
            // remove this from nodeItr's parents
            for (auto it = (*nodeItr)->parents.begin(); it != (*nodeItr)->parents.end(); it++) {
                if ((*it).get() == this) {
                    (*nodeItr)->parents.erase(it);
                    break;
                }
            }
            // remove nodeItr from children
            children.erase(nodeItr);
            return true;
        }
    }
    NES_DEBUG("Node: node was not found and could not be removed from children.");
    return false;
}

bool Node::addParent(const NodePtr newNode) {
    if (newNode.get() == this) {
        NES_WARNING("Node: Adding node to its self so will skip add parent operation.");
        return false;
    }

    // checks if current new node is not part of parents
    if (vectorContainsTheNode(parents, newNode)) {
        NES_WARNING("Node: the node is already part of its parents so ignore add parent operation.");
        return false;
    }
    // add the node to the parents
    parents.push_back(newNode);
    if (!vectorContainsTheNode(newNode->children, shared_from_this())) {
        newNode->children.push_back(shared_from_this());
    }
    return true;
}

bool Node::insertBetweenThisAndParentNodes(const NodePtr newNode) {

    if (newNode.get() == this) {
        NES_WARNING("Node:  Adding node to its self so will skip insertBetweenThisAndParentNodes operation.");
        return false;
    }

    if (vectorContainsTheNode(parents, newNode)) {
        NES_WARNING("Node: the node is already part of its parents so ignore insertBetweenThisAndParentNodes operation.");
        return false;
    }

    NES_INFO("Node: Create temporary copy of this nodes parents.");
    std::vector<NodePtr> copyOfParents = parents;

    NES_INFO("Node: Remove all parents of this node.");
    removeAllParent();

    if (!addParent(newNode)) {
        NES_ERROR("Node: Unable to add input node as parent to this node.");
        return false;
    }

    NES_INFO("Node: Add copy of this nodes parent as parent to the input node.");
    for (NodePtr parent : copyOfParents) {
        if (!newNode->addParent(parent)) {
            NES_ERROR("Node: Unable to add parent of this node as parent to input node.");
            return false;
        }
    }

    return true;
}

void Node::removeAllParent() {
    NES_INFO("Node: Removing all parents for current node");
    for (auto nodeItr = parents.begin(); nodeItr != parents.end(); ++nodeItr) {
        this->removeParent(*nodeItr);
        NES_INFO("Node: Removed node as parent of this node");
    }
}

void Node::removeChildren() {
    NES_INFO("Node: Removing all children for current node");
    for (auto nodeItr = children.begin(); nodeItr != children.end(); ++nodeItr) {
        this->removeChild(*nodeItr);
        NES_INFO("Node: Removed node as child of this node");
    }
}

bool Node::removeParent(const NodePtr node) {
    // check all parents.
    for (auto nodeItr = parents.begin(); nodeItr != parents.end(); ++nodeItr) {
        if ((*nodeItr)->equal(node)) {
            for (auto it = (*nodeItr)->children.begin(); it != (*nodeItr)->children.end(); it++) {
                if ((*it).get() == this) {
                    (*nodeItr)->children.erase(it);
                    break;
                }
            }
            parents.erase(nodeItr);
            return true;
        }
    }
    NES_DEBUG("Node: node was not found and could not be removed from parents.");
    return false;
}

bool Node::replace(NodePtr newNode) {
    return replace(newNode, shared_from_this());
}

bool Node::replace(NodePtr newNode, NodePtr oldNode) {
    if (shared_from_this() == oldNode) {
        insertBetweenThisAndParentNodes(newNode);
        removeAndJoinParentAndChildren();
        return true;
    }

    if (oldNode->isIdentical(newNode)) {
        NES_WARNING("Node: the new node was the same so will skip replace operation.");
        return true;
    }

    if (!oldNode->equal(newNode)) {
        // newNode is already inside children or parents and it's not oldNode
        if (find(children, newNode) || find(parents, newNode)) {
            NES_DEBUG("Node: the new node is already part of the children or predessessors of the current node.");
            return false;
        }
    }

    bool success = removeChild(oldNode);
    if (success) {
        children.push_back(newNode);
        for (auto&& currentNode : oldNode->children) {
            newNode->addChild(currentNode);
        }
        return true;
    } else {
        NES_ERROR("Node: could not remove child from  old node:" << oldNode->toString());
    }

    success = removeParent(oldNode);
    NES_DEBUG("Node: remove parent old node:" << oldNode->toString());
    if (success) {
        parents.push_back(newNode);
        for (auto&& currentNode : oldNode->parents) {
            newNode->addParent(currentNode);
        }
        return true;//TODO: I think this is wrong
    } else {
        NES_ERROR("Node: could not remove parent from  old node:" << oldNode->toString());
    }

    return false;
}

bool Node::swap(const NodePtr newNode, const NodePtr oldNode) {
    auto node = findRecursively(shared_from_this(), oldNode);
    // oldNode is not in current graph
    if (!node) {
        return false;
    }
    // detecting if newNode is one of oldNode's sblings
    for (auto&& parent : node->parents) {
        for (auto&& child : parent->children) {
            if (child == newNode) {
                // we don't want to handle this case
                return false;
            }
        }
    }

    // reset all parents belongs to newNode
    newNode->parents.clear();
    uint64_t criteria = 0;
    while (node->parents.size() > criteria) {
        auto parent = node->parents[criteria];
        if (parent.get() == newNode.get()) {
            criteria = 1;
            continue;
        }

        newNode->addParent(parent);
        node->removeParent(parent);
    }

    return true;
}

bool Node::remove(const NodePtr node) {
    // NOTE: if there is a cycle inside the operator topology, it won't behave correctly.
    return removeChild(node) || removeParent(node);
}

bool Node::removeAndLevelUpChildren(const NodePtr node) {

    // if a successor of node is equal to children,
    // it's confused to merge two equal operators,
    // HERE we don't deal with this case
    for (auto&& n : node->children) {
        if (find(children, n)) {
            return false;
        }
    }

    bool success = removeChild(node);
    if (success) {
        for (auto&& n : node->children) {
            children.push_back(n);
        }
        return true;
    }
    return false;
}

bool Node::removeAndJoinParentAndChildren() {
    try {
        NES_DEBUG("Node: Joining parents with children");

        std::vector<NodePtr> childCopy = this->children;
        std::vector<NodePtr> parentCopy = this->parents;
        for (auto& parent : parentCopy) {
            for (auto& child : childCopy) {

                NES_DEBUG("Node: Add child of this node as child of this node's parent");
                parent->addChild(child);

                NES_DEBUG("Node: remove this node as parent of the child");
                child->removeParent(shared_from_this());
            }
            parent->removeChild(shared_from_this());
            NES_DEBUG("Node: remove this node as child of this node's parents");
        }
        return true;
    } catch (...) {
        NES_ERROR("Node: Error ocurred while joining this node's children and parents");
        return false;
    }
}

void Node::clear() {
    children.clear();
    parents.clear();
}

const std::vector<NodePtr>& Node::getChildren() const {
    return children;
}

bool Node::containAsChild(NodePtr node) {
    NES_DEBUG("Node: Checking if the input node is contained in the children list");
    return vectorContainsTheNode(children, node);
}

bool Node::containAsParent(NodePtr node) {
    NES_DEBUG("Node: Checking if the input node is contained in the parent list");
    return vectorContainsTheNode(parents, node);
}

const std::vector<NodePtr>& Node::getParents() const {
    return parents;
}

std::vector<NodePtr> Node::getAllRootNodes() {
    NES_DEBUG("Node: Get all root nodes for this node");
    std::vector<NodePtr> rootNodes;

    if (getParents().empty()) {
        NES_DEBUG("Node: Inserting this node to the collection");
        rootNodes.push_back(shared_from_this());
    }

    for (auto& parent : parents) {
        if (parent->getParents().empty()) {
            NES_DEBUG("Node: Inserting root node to the collection");
            rootNodes.push_back(parent);
        } else {
            NES_DEBUG("Node: Iterating over all parents to find more root nodes");
            for (auto& parentOfParent : parent->getParents()) {
                std::vector<NodePtr> parentNodes = parentOfParent->getAllRootNodes();
                NES_DEBUG("Node: inserting parent nodes into the collection of parent nodes");
                rootNodes.insert(rootNodes.end(), parentNodes.begin(), parentNodes.end());
            }
        }
    }
    NES_DEBUG("Node: Found " << rootNodes.size() << " leaf nodes");
    return rootNodes;
}

std::vector<NodePtr> Node::getAllLeafNodes() {
    NES_DEBUG("Node: Get all leaf nodes for this node");
    std::vector<NodePtr> leafNodes;

    if (children.empty()) {
        NES_DEBUG("Node: found no children. Returning itself as leaf.");
        leafNodes.push_back(shared_from_this());
    }

    for (auto& child : children) {
        if (child->getChildren().empty()) {
            NES_DEBUG("Node: Inserting leaf node to the collection");
            leafNodes.push_back(child);
        } else {
            NES_DEBUG("Node: Iterating over all children to find more leaf nodes");
            for (auto& childOfChild : child->getChildren()) {
                std::vector<NodePtr> childrenLeafNodes = childOfChild->getAllLeafNodes();
                NES_DEBUG("Node: inserting leaf nodes into the collection of leaf nodes");
                leafNodes.insert(leafNodes.end(), childrenLeafNodes.begin(), childrenLeafNodes.end());
            }
        }
    }
    NES_DEBUG("Node: Found " << leafNodes.size() << " leaf nodes");
    return leafNodes;
}

bool Node::vectorContainsTheNode(const std::vector<NodePtr>& nodes, const NES::NodePtr nodeToFind) {
    return find(nodes, nodeToFind) != nullptr;
}

/**
 * Note that, operators with same description are not necessarily the same object.
 * @param nodes
 * @param nodeToFind
 * @return
 */
NodePtr Node::find(const std::vector<NodePtr>& nodes, const NodePtr nodeToFind) {
    for (auto&& currentNode : nodes) {
        if (nodeToFind->equal(currentNode)) {// TODO: need to check this when merge is used. nodeToFind.get() == currentNode.get()
            return currentNode;
        }
    }
    return nullptr;
}

NodePtr Node::findRecursively(const NodePtr root, const NodePtr nodeToFind) {
    // DFS
    NodePtr resultNode = nullptr;
    // two operator are equal, may not the same object
    if (root->isIdentical(nodeToFind)) {
        return root;
    }

    // not equal
    for (auto& currentNode : root->children) {
        resultNode = findRecursively(currentNode, nodeToFind);
        if (resultNode) {
            break;
        }
    }
    return resultNode;
}

bool Node::equalWithAllChildrenHelper(const NodePtr node1, const NodePtr node2) {
    if (node1->children.size() != node2->children.size())
        return false;

    auto x = node1->children.begin();
    while (x != node1->children.end()) {
        auto y = node2->children.begin();
        while (y != node2->children.end()) {
            if (x[0]->equal(y[0])) {
                if (!equalWithAllChildrenHelper(x[0], y[0])) {
                    return false;
                }
                break;
            }
            ++y;
        }
        if (y == node2->children.end()) {
            return false;
        }
        ++x;
    }
    return true;
}

bool Node::equalWithAllChildren(const NodePtr otherNode) {
    // the root is equal
    if (!equal(otherNode)) {
        return false;
    }

    return equalWithAllChildrenHelper(shared_from_this(), otherNode);
}// namespace NES

bool Node::equalWithAllParentsHelper(const NodePtr node1, const NodePtr node2) {
    if (node1->parents.size() != node2->parents.size())
        return false;

    auto x = node1->parents.begin();

    while (x != node1->parents.end()) {
        auto y = node2->parents.begin();
        while (y != node2->parents.end()) {
            if ((*x)->equal(*y)) {
                if (!equalWithAllParentsHelper(*x, *y)) {
                    return false;
                }
                break;
            }
            ++y;
        }
        if (y == node2->parents.end()) {
            return false;
        }
        ++x;
    }
    return true;
}

bool Node::equalWithAllParents(const NodePtr node) {
    // the root is equal
    if (!equal(node)) {
        return false;
    }
    return equalWithAllParentsHelper(shared_from_this(), node);
}

std::vector<NodePtr> Node::split(const NodePtr splitNode) {
    std::vector<NodePtr> result{};
    auto node = findRecursively(shared_from_this(), splitNode);
    if (!node) {
        NES_DEBUG("Node: operator is not in graph so dont split.");
        result.push_back(shared_from_this());
        return result;
    }

    while (!node->parents.empty()) {
        auto p = node->parents[0];
        result.push_back(p);
        node->removeParent(p);
    }
    result.push_back(node);
    return result;
}

bool Node::isValid() {
    return !isCyclic();
}

std::vector<NodePtr> Node::getAndFlattenAllChildren() {
    std::vector<NodePtr> allChildren{};
    getAndFlattenAllChildrenHelper(shared_from_this(), allChildren, shared_from_this());
    return allChildren;
}

void Node::getAndFlattenAllChildrenHelper(const NodePtr node, std::vector<NodePtr>& allChildren, const NodePtr excludednode) {

    // todo this implementation may be slow
    for (auto&& currentNode : node->children) {
        if (!find(allChildren, currentNode) && (currentNode != excludednode)) {
            allChildren.push_back(currentNode);
            getAndFlattenAllChildrenHelper(currentNode, allChildren, excludednode);
        }
    }
}

std::vector<NodePtr> Node::getAndFlattenAllAncestors() {
    NES_INFO("Node: Get this node, all its parents, and Ancestors");
    std::vector<NodePtr> result{shared_from_this()};
    for (auto& parent : parents) {
        NES_TRACE("Node: Get this node, all its parents, and Ancestors");
        std::vector<NodePtr> parentAndAncestors = parent->getAndFlattenAllAncestors();
        NES_TRACE("Node: Add them to the result");
        result.insert(result.end(), parentAndAncestors.begin(), parentAndAncestors.end());
    }
    return result;
}

bool Node::isCyclic() {
    auto allChildren = getAndFlattenAllChildren();
    for (auto&& node : allChildren) {
        node->visited = false;
        node->recStack = false;
    }

    // since *this is not in allChildren vector
    // we test it individually
    if (isCyclicHelper(*this)) {
        return true;
    }

    // test all sub-node in the DAG
    for (auto&& node : allChildren) {
        if (isCyclicHelper(*node)) {
            for (auto&& node : allChildren) {
                node->visited = false;
                node->recStack = false;
            }
            return true;
        }
    }
    for (auto&& node : allChildren) {
        node->visited = false;
        node->recStack = false;
    }
    return false;
}

bool Node::isCyclicHelper(Node& node) {
    // DFS
    node.visited = true;
    node.recStack = true;
    for (auto&& n : node.children) {
        if (!n->visited && isCyclicHelper(*n.get())) {
            return true;
        } else if (n->recStack) {
            return true;
        }
    }
    node.recStack = false;
    return false;
}
const std::vector<std::string> Node::toMultilineString() {
    std::vector<std::string> lines;
    lines.push_back(toString());
    return lines;
}

}// namespace NES
