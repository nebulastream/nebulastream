#include <OperatorNodes/BaseOperatorNode.hpp>

namespace NES {

BaseOperatorNode::BaseOperatorNode() {

}

BaseOperatorNode::~BaseOperatorNode() {

}
// BaseOperatorNode& BaseOperatorNode::operator=(BaseOperatorNode& other) {
//     if (this != &other) {

//     }
//     return *this;
// }

void BaseOperatorNode::addSuccessor(BaseOperatorNodePtr op) {
    // std::cout << "Add Successor: " << op->toString() << std::endl;
    this->successors.push_back(op);
}

void BaseOperatorNode::addPredecessor(BaseOperatorNodePtr op) {
    // std::cout << "Add Predecessor: " << op->toString() << std::endl;
    this->predecessors.push_back(op);
}

bool BaseOperatorNode::removeSuccessor(BaseOperatorNodePtr op) {
    int offset = 0;
    for (auto && o : this->successors) {
        if (o->getOperatorId() == op->getOperatorId()) {
            break;
        }
        offset ++;
    }
    if (offset < this->successors.size()) {
        this->successors.erase(this->successors.begin() + offset);
        return true;
    } else {
        return false;
    }
}

bool BaseOperatorNode::removePredecessor(BaseOperatorNodePtr op) {
    int offset = 0;
    for (auto && o : this->predecessors) {
        if (o->getOperatorId() == op->getOperatorId()) {
            break;
        }
        offset ++;
    }
    if (offset < this->predecessors.size()) {
        this->predecessors.erase(this->predecessors.begin() + offset);
        return true;
    } else {
        return false;
    }
}
bool BaseOperatorNode::replace(BaseOperatorNodePtr newOp, BaseOperatorNodePtr oldOp) {
    bool succ = false;
    succ = removeSuccessor(oldOp);
    if (succ) {
        this->successors.push_back(newOp);
        return true;
    }
    succ = removePredecessor(oldOp);
    if (succ) {
        this->predecessors.push_back(newOp);
        return true;
    }
    return false;
}

bool BaseOperatorNode::remove(BaseOperatorNodePtr op) {
    if (! this->removeSuccessor(op)) {
        return this->removePredecessor(op);
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
    this->operatorId = id;
}


}
