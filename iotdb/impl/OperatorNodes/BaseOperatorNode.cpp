#include <queue>
#include <unordered_set>
#include <Util/UtilityFunctions.hpp>
#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

BaseOperatorNode::BaseOperatorNode() : operatorId(UtilityFunctions::generateUuid()) {
}

BaseOperatorNode::~BaseOperatorNode() {

}

void BaseOperatorNode::addSuccessor(const BaseOperatorNodePtr& op) {
    assert(op);
    if (op.get() == this)
        return;

    bool found = this->find(this->successors, op);
    if (! found)
        this->successors.push_back(op);
}

void BaseOperatorNode::addPredecessor(const BaseOperatorNodePtr& op) {
    assert(op);
    if (op.get() == this)
        return;

    bool found = this->find(this->predecessors, op);
    if (! found)
        this->predecessors.push_back(op);
}

bool BaseOperatorNode::removeSuccessor(const BaseOperatorNodePtr& op) {
    assert(op);

    for (auto opIt = this->successors.begin(); opIt != this->successors.end(); ++ opIt) {
        if ((*opIt)->equals(*op.get())) {
            this->successors.erase(opIt);
            return true;
        }
    }
    return false;
}

bool BaseOperatorNode::removePredecessor(const BaseOperatorNodePtr& op) {
    assert(op);

    for (auto opIt = this->predecessors.begin(); opIt != this->predecessors.end(); ++ opIt) {
        if ((*opIt)->equals(*op.get())) {
            this->predecessors.erase(opIt);
            return true;
        }
    }
    return false;
}

bool BaseOperatorNode::replace(BaseOperatorNodePtr newOp, BaseOperatorNodePtr oldOp) {
    assert(oldOp);
    assert(newOp);

    if (oldOp->isIdentical(*newOp.get())) {
        return true;
    }

    if (*oldOp.get() != *newOp.get()) {
        // newOp is already inside successors or predecessors and it's not oldOp
        // raise exception ?
        if (find(this->successors, newOp) ||
            find(this->predecessors, newOp)) {
            return false;
        }
    }

    bool succ = false;
    succ = removeSuccessor(oldOp);
    if (succ) {
        this->successors.push_back(newOp);
        for (auto && op_ : oldOp->successors) {
            newOp->addSuccessor(op_);
        }
        return true;
    }
    succ = removePredecessor(oldOp);
    if (succ) {
        this->predecessors.push_back(newOp);
        for (auto && op_ : oldOp->predecessors) {
            newOp->addPredecessor(op_);
        }
        return true;
    }
    return false;
}

bool BaseOperatorNode::remove(const BaseOperatorNodePtr& op) {
    // NOTE: if there is a ring inside the operator topology, it won't behave correctly.
    return this->removeSuccessor(op) || this->removePredecessor(op);
}

bool BaseOperatorNode::removeAndLevelUpSuccessors(const BaseOperatorNodePtr& op) {
    assert(op);

    // if a successor of op is equal to this->successors,
    // it's confused to merge two equal operators,
    // HERE we don't deal with this case
    for (auto && op_ : op->successors) {
        if (find(this->successors, op_)) {
            return false;
        }
    }

    bool succ = false;
    succ = removeSuccessor(op);
    if (succ) {
        for (auto && op_ : op->successors) {
            this->successors.push_back(op_);
        }
        return true;
    }
    return false;
}

void BaseOperatorNode::clear() {
    this->successors.clear();
    this->predecessors.clear();
}

const std::vector<BaseOperatorNodePtr>& BaseOperatorNode::getSuccessors() const {
    return this->successors;
}

const std::vector<BaseOperatorNodePtr>& BaseOperatorNode::getPredecessors() const {
    return this->predecessors;
}

// size_t BaseOperatorNode::getOperatorId() const {
//     return this->operatorId;
// }
// void BaseOperatorNode::setOperatorId(const size_t id) {
//     this->operatorId = id;
// }
const std::string BaseOperatorNode::getOperatorId() const {
    return this->operatorId;
}

void BaseOperatorNode::setOperatorId(const std::string& id) {
    // this->operatorId = id;
}

bool BaseOperatorNode::find(const std::vector<BaseOperatorNodePtr>& operatorNodes, const BaseOperatorNodePtr& op) {
    for (auto&& op_ : operatorNodes) {
        if (op->equals(*op_.get())) {
            return true;
        }
    }
    return false;
}

bool BaseOperatorNode::equalWithAllSuccessorsHelper(const BaseOperatorNode& op1, const BaseOperatorNode& op2) {
    if (op1.successors.size() != op2.successors.size())
        return false;

    auto x = op1.successors.begin();

    while (x != op1.successors.end()) {
        auto y = op2.successors.begin();
        while (y != op2.successors.end()) {
            if ((*x)->equals(*(*y).get())) {
                if(! equalWithAllSuccessorsHelper(*(*x).get(), *(*y).get())) {
                    return false;
                }
                break;
            }
            ++ y;
        }
        if (y == op2.successors.end()) {
            return false;
        }
        ++ x;
    }
    return true;
}

bool BaseOperatorNode::equalWithAllSuccessors(const BaseOperatorNodePtr& op) {
    // the root is equal
    if (! this->equals(*op.get()))
        return false;

    return equalWithAllSuccessorsHelper(*this, *op.get());
}

bool BaseOperatorNode::equalWithAllPredecessorsHelper(const BaseOperatorNode& op1, const BaseOperatorNode& op2) {
    if (op1.predecessors.size() != op2.predecessors.size())
        return false;

    auto x = op1.predecessors.begin();

    while (x != op1.predecessors.end()) {
        auto y = op2.predecessors.begin();
        while (y != op2.predecessors.end()) {
            if ((*x)->equals(*(*y).get())) {
                if (! equalWithAllPredecessorsHelper(*(*x).get(), *(*y).get())) {
                    return false;
                }
                break;
            }
            ++ y;
        }
        if (y == op2.predecessors.end()) {
            return false;
        }
        ++ x;
    }

    return true;

}

bool BaseOperatorNode::equalWithAllPredecessors(const BaseOperatorNodePtr& op) {
    // the root is equal
    if (! this->equals(*op.get()))
        return false;

    return equalWithAllPredecessorsHelper(*this, *op.get());
}

}
