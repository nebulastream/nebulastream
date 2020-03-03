#include <queue>
#include <unordered_set>
#include <Util/UtilityFunctions.hpp>
#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

BaseOperatorNode::BaseOperatorNode() : operatorId(UtilityFunctions::generateUuid()), visited(false), recStack(false) {
}

BaseOperatorNode::~BaseOperatorNode() {

}

void BaseOperatorNode::addSuccessor(const BaseOperatorNodePtr& op) {
    assert(op);
    if (op.get() == this)
        return;

    bool found = (this->find(this->successors, op) != nullptr);
    if (! found) {
        this->successors.push_back(op);
        if (! find(op->predecessors, this->makeShared()))
            op->predecessors.push_back(this->makeShared());
    }
}

void BaseOperatorNode::addPredecessor(const BaseOperatorNodePtr& op) {
    assert(op);
    if (op.get() == this)
        return;

    bool found = (this->find(this->predecessors, op) != nullptr);
    if (! found) {
        this->predecessors.push_back(op);
        if (! find(op->successors, this->makeShared()))
            op->successors.push_back(this->makeShared());
    }
}

bool BaseOperatorNode::removeSuccessor(const BaseOperatorNodePtr& op) {
    assert(op);

    for (auto opIt = this->successors.begin(); opIt != this->successors.end(); ++ opIt) {
        if ((*opIt)->equals(*op.get())) {
            // remove this from opIt's predecessors
            std::cout << "size of predecessors 1: " << (*opIt)->predecessors.size() << std::endl;
            for (auto it = (*opIt)->predecessors.begin(); it != (*opIt)->predecessors.end(); it++) {
                if ((*it).get() == this) {
                    (*opIt)->predecessors.erase(it);
                    break;
                }
            }
            std::cout << "size of predecessors 2: " << (*opIt)->predecessors.size() << std::endl;

            // remove opIt from successors
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
            // std::cout << "--------------------------------------------------------------------------------\n";
            // std::cout << "call erase opIt " << (*opIt)->toString() << std::endl;
            // std::cout << "size of successors 1: " << (*opIt)->predecessors.size() << std::endl;
            for (auto it = (*opIt)->successors.begin(); it != (*opIt)->successors.end(); it++) {
                // std::cout << "call erase it " << (*it)->toString() << std::endl;
                if ((*it).get() == this) {
                    (*opIt)->successors.erase(it);
                    break;
                }
            }
            // std::cout << "size of successors 2: " << (*opIt)->predecessors.size() << std::endl;
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

BaseOperatorNodePtr BaseOperatorNode::find(const std::vector<BaseOperatorNodePtr>& operatorNodes, const BaseOperatorNodePtr& op) {
    for (auto&& op_ : operatorNodes) {
        if (op->equals(*op_.get())) {
            return op_;
        }
    }
    return nullptr;
}

BaseOperatorNodePtr BaseOperatorNode::findRecursively(BaseOperatorNode& root, BaseOperatorNode& op) {
    // DFS
    BaseOperatorNodePtr x = nullptr;
    // two operator are equal, may not the same object
    if (root == op)
        return root.makeShared();

    // not equal
    for (auto && op_ : root.successors) {
        x = findRecursively(*op_.get(), op);
        if (x) {
            break;
        }
    }
    return x;
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

std::vector<BaseOperatorNodePtr> BaseOperatorNode::getOperatorsByType(const OperatorType& type) {
        std::vector<BaseOperatorNodePtr> vec {};
    if (this->getOperatorType() == type) {
        vec.push_back(this->makeShared());
    }
    getOperatorsByTypeHelper(*this, vec, *this, type);
    return std::move(vec);
}

void BaseOperatorNode::getOperatorsByTypeHelper(BaseOperatorNode& op,
                                                std::vector<BaseOperatorNodePtr>& allChildren,
                                                BaseOperatorNode& excludedOp,
                                                const OperatorType& type) {
    // NOTE: poor performance
    for (auto && op_ : op.successors) {
        if (! find(allChildren, op_) &&
            (op_.get() != &excludedOp) &&
            op_->getOperatorType() == type) {
            allChildren.push_back(op_);
            // getAndFlattenAllSuccessorsHelper(*op_.get(), allChildren, excludedOp);
            getOperatorsByTypeHelper(*op_.get(), allChildren, excludedOp, type);
        }
    }
}


std::vector<BaseOperatorNodePtr> BaseOperatorNode::split(const BaseOperatorNodePtr& op) {
    auto op_ = findRecursively(*this, *op.get());
    if (! op_) {
        throw std::invalid_argument("received operator not in graph.");
    }
    std::vector<BaseOperatorNodePtr> vec {};

    while (op_->predecessors.size() > 0) {
        auto p = op_->predecessors[0];
        vec.push_back(p);
        // p->removeSuccessor(x);
        op_->removePredecessor(p);
    }
    vec.push_back(op_);
    return vec;
}

bool BaseOperatorNode::swap(const BaseOperatorNodePtr& newOp, const BaseOperatorNodePtr& oldOp) {
    auto op = findRecursively(*this, *oldOp.get());
    // oldOp is not in current graph
    if (! op) {
        return false;
    }
    // detecting if newOp is one of oldOp's sblings
    for (auto&& op_ : op->predecessors) {
        for (auto&& op__ : op_->successors) {
            if (*op__.get() == *newOp.get()) {
                // we don't want to handle this case
                return false;
            }
        }
    }

    // reset all predecessors belongs to newOp
    newOp->predecessors.clear();
    int criteria = 0;
    while (op->predecessors.size() > criteria) {
        // std::cout << "op->size: " << op->predecessors.size() << std::endl;
        auto p = op->predecessors[criteria];
        if (p.get() == newOp.get()) {
            criteria = 1;
            continue;
        }

        newOp->addPredecessor(p);
        op->removePredecessor(p);
            // std::cout << "ERROR " << std::endl;
        // }
    }

    return true;
}

bool BaseOperatorNode::instanceOf(const OperatorType& type) {
    if (this->getOperatorType() == type)
        return true;
    return false;
}

bool BaseOperatorNode::isValid() {
    return ! isCyclic();
}

std::vector<BaseOperatorNodePtr>BaseOperatorNode::getAndFlattenAllSuccessors() {
    std::vector<BaseOperatorNodePtr> allChildren {};
    getAndFlattenAllSuccessorsHelper(*this, allChildren, *this);
    return allChildren;
}

void BaseOperatorNode::getAndFlattenAllSuccessorsHelper(BaseOperatorNode& op,
                                                        std::vector<BaseOperatorNodePtr>& allChildren, BaseOperatorNode& excludedOp) {
                                                        // bool predicateFunc(std::vector<BaseOperatorNodePtr>&,
                                                                           // BaseOperatorNodePtr& op,
                                                                           // BaseOperatorNode& excludedOp,
                                                                           // OperatorType& type)) {
    // NOTE: poor performance
    for (auto && op_ : op.successors) {
        if (! find(allChildren, op_) &&
            (op_.get() != &excludedOp)) {
            allChildren.push_back(op_);
            getAndFlattenAllSuccessorsHelper(*op_.get(), allChildren, excludedOp);
        }
    }
}

bool BaseOperatorNode::isCyclic() {
    auto allChildren = getAndFlattenAllSuccessors();
    for (auto && op : allChildren) {
        op->visited = false;
        op->recStack = false;
    }

    // since *this is not in allChildren vector
    // we test it individually
    if (isCyclicHelper(*this))
        return true;

    // test all sub-node in the DAG
    for (auto && op : allChildren) {
        if (isCyclicHelper(*op.get())) {
            for (auto && op : allChildren) {
                op->visited = false;
                op->recStack = false;
            }
            return true;
        }

    }
    for (auto && op : allChildren) {
        op->visited = false;
        op->recStack = false;
    }
    return false;
}

bool BaseOperatorNode::isCyclicHelper(BaseOperatorNode& op) {
    // DFS
    op.visited = true;
    op.recStack = true;
    for (auto && op_ : op.successors) {
        if (! op_->visited && this->isCyclicHelper(*op_.get()))
            return true;
        else if (op_->recStack)
            return true;
    }
    op.recStack = false;
    return false;
}
void BaseOperatorNode::prettyPrint(std::ostream& out) const {
    this->printHelper(*this, 0, 2, out);
}

void BaseOperatorNode::printHelper(const BaseOperatorNode& op, size_t depth, size_t indent, std::ostream& out) const {

    out << std::string(indent*depth, ' ') << op.toString()
        // << ", <#id: " << op.getOperatorId() << ">"
        << std::endl;
    ++ depth;
    auto children = op.getSuccessors();
    for (auto&& child: children) {
        printHelper(*child.get(), depth, indent, out);
    }
}
}
