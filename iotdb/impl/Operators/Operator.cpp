
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
  result.insert(this->getOperatorType());
  if (!childs.empty()) {
    for (const OperatorPtr &op: childs) {
      std::set<OperatorType> tmp = op->flattenedTypes();
      result.insert(tmp.begin(), tmp.end());
    }
  }
  return result;
}
bool Operator::equals(const Operator &_rhs) {
  //TODO: change equals method to virtual bool equals(const Operator &_rhs) = 0;
  assert(0);
  return false;
}
} // namespace iotdb
