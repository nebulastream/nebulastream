
#include <CodeGen/CodeGen.hpp>
#include <Operators/Operator.hpp>
#include <Util/ErrorHandling.hpp>
#include <sstream>
#include <SourceSink/DataSource.hpp>
#include <set>

namespace iotdb {

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
    if (!this->childs.empty()) {
      result.insert(this->getOperatorType());
      for (const OperatorPtr &_op: this->childs) {
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

bool Operator::equals(const Operator &_rhs) {
  //TODO: change equals method to virtual bool equals(const Operator &_rhs) = 0;
  assert(0);
  return false;
}
} // namespace iotdb
