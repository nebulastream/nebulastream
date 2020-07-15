
#include <Operators/Operator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <Sources/DataSource.hpp>
#include <assert.h>
#include <set>
#include <sstream>

namespace NES {

Operator::~Operator() {}

std::set<OperatorType> Operator::flattenedTypes() {
    std::set<OperatorType> result;

    std::set<OperatorType> tmpParent = traverseOpTree(false);
    result.insert(tmpParent.begin(), tmpParent.end());

    result.insert(this->getOperatorType());

    std::set<OperatorType> tmpChild = traverseOpTree(true);
    result.insert(tmpChild.begin(), tmpChild.end());

    return result;
}

std::set<OperatorType> Operator::traverseOpTree(bool traverse_children) {
    std::set<OperatorType> result;
    if (traverse_children) {
        if (!this->children.empty()) {
            result.insert(this->getOperatorType());
            for (const OperatorPtr& _op : this->children) {
                std::set<OperatorType> tmp = _op->traverseOpTree(traverse_children);
                result.insert(tmp.begin(), tmp.end());
            }
        }
    } else {
        if (this->parent) {
            std::set<OperatorType> tmp = this->parent->traverseOpTree(traverse_children);
            result.insert(tmp.begin(), tmp.end());
        }
        result.insert(this->getOperatorType());
    }
    return result;
}

bool Operator::equals(const Operator& _rhs) {
    //TODO: change equals method to virtual bool equals(const Operator &_rhs) = 0;
    assert(0);
    return false;
}

const std::vector<OperatorPtr> Operator::getChildren() const {
    return children;
}

void Operator::setChildren(const std::vector<OperatorPtr> children) {
    this->children = children;
}

void Operator::addChild(const OperatorPtr child) {
    this->children.push_back(child);
}

const OperatorPtr Operator::getParent() const {
    return parent;
}

void Operator::setParent(const OperatorPtr parent) {
    this->parent = parent;
}

}// namespace NES
