#include <queue>
#include <unordered_set>
#include <Util/UtilityFunctions.hpp>
#include <OperatorNodes/Node.hpp>

namespace NES {

Node::Node() : visited(false), recStack(false) {
}

Node::~Node() {

}

void Node::addSuccessor(const NodePtr& newNode) {
    assert(newNode);
    if (newNode.get() == this) {
        NES_DEBUG("Node: Added note to its self, so we ignore this operation.");
        return;
    }
    // checks if current new node is not part of successors
    bool found = (this->find(this->successors, newNode) != nullptr);
    if (found) {
        NES_DEBUG("Node: the node is already part of its successors so we ignore it.");
        return;
    }
    // add the node to the successors
    this->successors.push_back(newNode);

    // add the current node as a predecessors to other node
    if (!find(newNode->predecessors, this->shared_from_this())) {
        newNode->predecessors.push_back(this->shared_from_this());
    }
}

bool Node::removeSuccessor(const NodePtr& node) {
    assert(node);

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

void Node::addPredecessor(const NodePtr& newNode) {
    assert(newNode);
    if (newNode.get() == this) {
        NES_DEBUG("Node: Added note to its self, so we ignore this operation.");
        return;
    }

    // checks if current new node is not part of predecessors
    bool found = (this->find(this->predecessors, newNode) != nullptr);
    if (found) {
        NES_DEBUG("Node: the node is already part of its successors so we ignore it.");
        return;
    }
    // add the node to the predecessors
    this->predecessors.push_back(newNode);
    if (!find(newNode->successors, this->shared_from_this())) {
        newNode->successors.push_back(this->shared_from_this());
    }
}

bool Node::removePredecessor(const NodePtr& op) {
    assert(op);
    // check all predecessors.
    for (auto opIt = this->predecessors.begin(); opIt != this->predecessors.end(); ++opIt) {
        if ((*opIt)->equal(op)) {
            for (auto it = (*opIt)->successors.begin(); it != (*opIt)->successors.end(); it++) {
                if ((*it).get() == this) {
                    (*opIt)->successors.erase(it);
                    break;
                }
            }
            this->predecessors.erase(opIt);
            return true;
        }
    }
    NES_DEBUG("Node: node was not found and could not be removed from predecessors.");
    return false;
}

bool Node::replace(NodePtr newNode, NodePtr oldNode) {
    assert(oldNode);
    assert(newNode);

    if (oldNode->isIdentical(newNode)) {
        NES_DEBUG("Node: the new node was the same so we ignored the operation.");
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
    int criteria = 0;
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

bool Node::remove(const NodePtr& op) {
    // NOTE: if there is a cycle inside the operator topology, it won't behave correctly.
    return this->removeSuccessor(op) || this->removePredecessor(op);
}

bool Node::removeAndLevelUpSuccessors(const NodePtr& op) {
    assert(op);

    // if a successor of op is equal to this->successors,
    // it's confused to merge two equal operators,
    // HERE we don't deal with this case
    for (auto&& op_ : op->successors) {
        if (find(this->successors, op_)) {
            return false;
        }
    }

    bool succ = false;
    succ = removeSuccessor(op);
    if (succ) {
        for (auto&& op_ : op->successors) {
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

NodePtr Node::find(const std::vector<NodePtr>& operatorNodes, const NodePtr& op) {
    for (auto&& op_ : operatorNodes) {
        if (op->equal(op_)) {
            return op_;
        }
    }
    return nullptr;
}

NodePtr Node::findRecursively(const NodePtr& root, const NodePtr& op) {
    // DFS
    NodePtr x = nullptr;
    // two operator are equal, may not the same object
    if (root->isIdentical(op))
        return root;

    // not equal
    for (auto& op_ : root->successors) {
        x = findRecursively(op_, op);
        if (x) {
            break;
        }
    }
    return x;
}

bool Node::equalWithAllSuccessorsHelper(const NodePtr& op1, const NodePtr& op2) {
    if (op1->successors.size() != op2->successors.size())
        return false;

    auto x = op1->successors.begin();

    while (x != op1->successors.end()) {
        auto y = op2->successors.begin();
        while (y != op2->successors.end()) {
            if (x[0]->equal(y[0])) {
                if (!equalWithAllSuccessorsHelper(x[0], y[0])) {
                    return false;
                }
                break;
            }
            ++y;
        }
        if (y == op2->successors.end()) {
            return false;
        }
        ++x;
    }
    return true;
}

bool Node::equalWithAllSuccessors(const NodePtr& op) {
    // the root is equal
    if (!this->equal(op))
        return false;

    return equalWithAllSuccessorsHelper(shared_from_this(), op);
}

bool Node::equalWithAllPredecessorsHelper(const NodePtr& op1, const NodePtr& op2) {
    if (op1->predecessors.size() != op2->predecessors.size())
        return false;

    auto x = op1->predecessors.begin();

    while (x != op1->predecessors.end()) {
        auto y = op2->predecessors.begin();
        while (y != op2->predecessors.end()) {
            if ((*x)->equal(*y)) {
                if (!equalWithAllPredecessorsHelper(*x, *y)) {
                    return false;
                }
                break;
            }
            ++y;
        }
        if (y == op2->predecessors.end()) {
            return false;
        }
        ++x;
    }
    return true;
}

bool Node::equalWithAllPredecessors(const NodePtr& op) {
    // the root is equal
    if (!this->equal(op))
        return false;

    return equalWithAllPredecessorsHelper(shared_from_this(), op);
}

std::vector<NodePtr> Node::split(const NodePtr& op) {
    auto op_ = findRecursively(shared_from_this(), op);
    if (!op_) {
        throw std::invalid_argument("received operator not in graph.");
    }
    std::vector<NodePtr> vec{};

    while (op_->predecessors.size() > 0) {
        auto p = op_->predecessors[0];
        vec.push_back(p);
        op_->removePredecessor(p);
    }
    vec.push_back(op_);
    return vec;
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

    // NOTE: poor performance
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
    for (auto&& op : allChildren) {
        op->visited = false;
        op->recStack = false;
    }

    // since *this is not in allChildren vector
    // we test it individually
    if (isCyclicHelper(*this))
        return true;

    // test all sub-node in the DAG
    for (auto&& op : allChildren) {
        if (isCyclicHelper(*op.get())) {
            for (auto&& op : allChildren) {
                op->visited = false;
                op->recStack = false;
            }
            return true;
        }

    }
    for (auto&& op : allChildren) {
        op->visited = false;
        op->recStack = false;
    }
    return false;
}

bool Node::isCyclicHelper(Node& op) {
    // DFS
    op.visited = true;
    op.recStack = true;
    for (auto&& op_ : op.successors) {
        if (!op_->visited && this->isCyclicHelper(*op_.get()))
            return true;
        else if (op_->recStack)
            return true;
    }
    op.recStack = false;
    return false;
}
void Node::prettyPrint(std::ostream& out) {
    this->printHelper(shared_from_this(), 0, 2, out);
}

void Node::printHelper(const NodePtr& op, size_t depth, size_t indent, std::ostream& out) const {

    out << std::string(indent*depth, ' ') << op->toString()
        // << ", <#id: " << op.getOperatorId() << ">"
        << std::endl;
    ++depth;
    auto children = op->getSuccessors();
    for (auto&& child: children) {
        printHelper(child, depth, indent, out);
    }
}
}
