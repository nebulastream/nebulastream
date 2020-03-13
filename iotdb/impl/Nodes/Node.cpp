#include <queue>
#include <unordered_set>
#include <Util/UtilityFunctions.hpp>
#include <Nodes/Node.hpp>

namespace NES {

Node::Node() : visited(false), recStack(false) {
}

Node::~Node() {

}

bool Node::addSuccessor(const NodePtr& newNode) {
    if (newNode.get() == this) {
        NES_DEBUG("Node: Added node to its self, ignore this operation.");
        return false;
    }
    // checks if current new node is not part of successors
    if (contains(this->successors, newNode)) {
        NES_DEBUG("Node: the node is already part of its successors so ignore it.");
        return false;
    }
    // add the node to the successors
    this->successors.push_back(newNode);

    // add the current node as a predecessors to the newNode
    if (!contains(newNode->predecessors, this->shared_from_this())) {
        newNode->predecessors.push_back(this->shared_from_this());
    }
    return true;
}

bool Node::removeSuccessor(const NodePtr& node) {
    // check all successors.
    for (auto nodeItr = this->successors.begin(); nodeItr != this->successors.end(); ++nodeItr) {
        if ((*nodeItr)->equal(node)) {
            // remove this from nodeItr's predecessors
            for (auto it = (*nodeItr)->predecessors.begin(); it != (*nodeItr)->predecessors.end(); it++) {
                if ((*it).get() == this) {
                    (*nodeItr)->predecessors.erase(it);
                    break;
                }
            }
            // remove nodeItr from successors
            this->successors.erase(nodeItr);
            return true;
        }
    }
    NES_DEBUG("Node: node was not found and could not be removed from successors.");
    return false;
}

bool Node::addPredecessor(const NodePtr& newNode) {
    if (newNode.get() == this) {
        NES_DEBUG("Node: Added node to its self, so ignore this operation.");
        return false;
    }

    // checks if current new node is not part of predecessors
    if (contains(this->predecessors, newNode)) {
        NES_DEBUG("Node: the node is already part of its predecessors so ignore it.");
        return false;
    }
    // add the node to the predecessors
    this->predecessors.push_back(newNode);
    if (!contains(newNode->successors, this->shared_from_this())) {
        newNode->successors.push_back(this->shared_from_this());
    }
    return true;
}

bool Node::removePredecessor(const NodePtr& node) {
    // check all predecessors.
    for (auto nodeItr = this->predecessors.begin(); nodeItr != this->predecessors.end(); ++nodeItr) {
        if ((*nodeItr)->equal(node)) {
            for (auto it = (*nodeItr)->successors.begin(); it != (*nodeItr)->successors.end(); it++) {
                if ((*it).get() == this) {
                    (*nodeItr)->successors.erase(it);
                    break;
                }
            }
            this->predecessors.erase(nodeItr);
            return true;
        }
    }
    NES_DEBUG("Node: node was not found and could not be removed from predecessors.");
    return false;
}

bool Node::replace(NodePtr newNode, NodePtr oldNode) {
    if (oldNode->isIdentical(newNode)) {
        NES_DEBUG("Node: the new node was the same so ignored the operation.");
        return true;
    }

    if (!oldNode->equal(newNode)) {
        // newNode is already inside successors or predecessors and it's not oldNode
        // raise exception ?
        if (find(this->successors, newNode) ||
            find(this->predecessors, newNode)) {
            NES_DEBUG("Node: the new node is already part of the successors or predessessors of the current node.");
            return false;
        }
    }
    // update successors and predecessors of new nodes.
    bool succ = false;
    succ = removeSuccessor(oldNode);
    if (succ) {
        this->successors.push_back(newNode);
        for (auto&& op_ : oldNode->successors) {
            newNode->addSuccessor(op_);
        }
        return true;
    }
    succ = removePredecessor(oldNode);
    if (succ) {
        this->predecessors.push_back(newNode);
        for (auto&& op_ : oldNode->predecessors) {
            newNode->addPredecessor(op_);
        }
        return true;
    }
    return false;
}

bool Node::swap(const NodePtr& newNode, const NodePtr& oldNode) {
    auto node = findRecursively(shared_from_this(), oldNode);
    // oldNode is not in current graph
    if (!node) {
        return false;
    }
    // detecting if newNode is one of oldNode's sblings
    for (auto&& op_ : node->predecessors) {
        for (auto&& op__ : op_->successors) {
            if (op__ == newNode) {
                // we don't want to handle this case
                return false;
            }
        }
    }

    // reset all predecessors belongs to newNode
    newNode->predecessors.clear();
    uint64_t criteria = 0;
    while (node->predecessors.size() > criteria) {
        auto p = node->predecessors[criteria];
        if (p.get() == newNode.get()) {
            criteria = 1;
            continue;
        }

        newNode->addPredecessor(p);
        node->removePredecessor(p);
    }

    return true;
}

bool Node::remove(const NodePtr& node) {
    // NOTE: if there is a cycle inside the operator topology, it won't behave correctly.
    return this->removeSuccessor(node) || this->removePredecessor(node);
}

bool Node::removeAndLevelUpSuccessors(const NodePtr& node) {

    // if a successor of node is equal to this->successors,
    // it's confused to merge two equal operators,
    // HERE we don't deal with this case
    for (auto&& op_ : node->successors) {
        if (find(this->successors, op_)) {
            return false;
        }
    }

    bool succ = false;
    succ = removeSuccessor(node);
    if (succ) {
        for (auto&& op_ : node->successors) {
            this->successors.push_back(op_);
        }
        return true;
    }
    return false;
}

void Node::clear() {
    this->successors.clear();
    this->predecessors.clear();
}

const std::vector<NodePtr>& Node::getSuccessors() const {
    return this->successors;
}

const std::vector<NodePtr>& Node::getPredecessors() const {
    return this->predecessors;
}

bool Node::contains(const std::vector<NodePtr>& nodes, const NES::NodePtr& nodeToFind) {
    return find(nodes, nodeToFind) != nullptr;
}

NodePtr Node::find(const std::vector<NodePtr>& nodes, const NodePtr& nodeToFind) {
    for (auto&& currentNode : nodes) {
        if (nodeToFind->equal(currentNode)) {
            return currentNode;
        }
    }
    return nullptr;
}

NodePtr Node::findRecursively(const NodePtr& root, const NodePtr& nodeToFind) {
    // DFS
    NodePtr x = nullptr;
    // two operator are equal, may not the same object
    if (root->isIdentical(nodeToFind))
        return root;

    // not equal
    for (auto& currentNode : root->successors) {
        x = findRecursively(currentNode, nodeToFind);
        if (x) {
            break;
        }
    }
    return x;
}

bool Node::equalWithAllSuccessorsHelper(const NodePtr& node1, const NodePtr& node2) {
    if (node1->successors.size() != node2->successors.size())
        return false;

    auto x = node1->successors.begin();
    while (x != node1->successors.end()) {
        auto y = node2->successors.begin();
        while (y != node2->successors.end()) {
            if (x[0]->equal(y[0])) {
                if (!equalWithAllSuccessorsHelper(x[0], y[0])) {
                    return false;
                }
                break;
            }
            ++y;
        }
        if (y == node2->successors.end()) {
            return false;
        }
        ++x;
    }
    return true;
}

bool Node::equalWithAllSuccessors(const NodePtr& node) {
    // the root is equal
    if (!this->equal(node))
        return false;

    return equalWithAllSuccessorsHelper(shared_from_this(), node);
}

bool Node::equalWithAllPredecessorsHelper(const NodePtr& node1, const NodePtr& node2) {
    if (node1->predecessors.size() != node2->predecessors.size())
        return false;

    auto x = node1->predecessors.begin();

    while (x != node1->predecessors.end()) {
        auto y = node2->predecessors.begin();
        while (y != node2->predecessors.end()) {
            if ((*x)->equal(*y)) {
                if (!equalWithAllPredecessorsHelper(*x, *y)) {
                    return false;
                }
                break;
            }
            ++y;
        }
        if (y == node2->predecessors.end()) {
            return false;
        }
        ++x;
    }
    return true;
}

bool Node::equalWithAllPredecessors(const NodePtr& node) {
    // the root is equal
    if (!this->equal(node))
        return false;

    return equalWithAllPredecessorsHelper(shared_from_this(), node);
}

std::vector<NodePtr> Node::split(const NodePtr& splitNode) {
    std::vector<NodePtr> result{};
    auto node = findRecursively(shared_from_this(), splitNode);
    if (!node) {
        NES_FATAL_ERROR("Node: operator is not in graph so dont split.")
        result.push_back(shared_from_this());
        return result;
    }

    while (!node->predecessors.empty()) {
        auto p = node->predecessors[0];
        result.push_back(p);
        node->removePredecessor(p);
    }
    result.push_back(node);
    return result;
}

bool Node::isValid() {
    return !isCyclic();
}

std::vector<NodePtr> Node::getAndFlattenAllSuccessors() {
    std::vector<NodePtr> allChildren{};
    getAndFlattenAllSuccessorsHelper(shared_from_this(), allChildren, shared_from_this());
    return allChildren;
}

void Node::getAndFlattenAllSuccessorsHelper(const NodePtr& op,
                                            std::vector<NodePtr>& allChildren, const NodePtr& excludedOp) {

    // todo this implementation may be slow
    for (auto&& op_ : op->successors) {
        if (!find(allChildren, op_) &&
            (op_ != excludedOp)) {
            allChildren.push_back(op_);
            getAndFlattenAllSuccessorsHelper(op_, allChildren, excludedOp);
        }
    }
}

bool Node::isCyclic() {
    auto allChildren = getAndFlattenAllSuccessors();
    for (auto&& node : allChildren) {
        node->visited = false;
        node->recStack = false;
    }

    // since *this is not in allChildren vector
    // we test it individually
    if (isCyclicHelper(*this))
        return true;

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
    for (auto&& n : node.successors) {
        if (!n->visited && this->isCyclicHelper(*n.get()))
            return true;
        else if (n->recStack)
            return true;
    }
    node.recStack = false;
    return false;
}
void Node::prettyPrint(std::ostream& out) {
    this->printHelper(shared_from_this(), 0, 2, out);
}

void Node::printHelper(const NodePtr& op, size_t depth, size_t indent, std::ostream& out) const {

    out << std::string(indent*depth, ' ') << op->toString() << std::endl;
    ++depth;
    auto children = op->getSuccessors();
    for (auto&& child: children) {
        printHelper(child, depth, indent, out);
    }
}
}
