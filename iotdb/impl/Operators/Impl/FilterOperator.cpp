

#include <iostream>
#include <QueryCompiler/CodeGen.hpp>
#include <Operators/Impl/FilterOperator.hpp>

namespace iotdb {

FilterOperator::FilterOperator(const PredicatePtr &predicate) : Operator(), predicate_(iotdb::copy(predicate)) {}

FilterOperator::FilterOperator(const FilterOperator &other) : predicate_(iotdb::copy(other.predicate_)) {}

FilterOperator &FilterOperator::operator=(const FilterOperator &other) {
  if (this != &other) {
    predicate_ = iotdb::copy(other.predicate_);
  }
  return *this;
}

void FilterOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream &out) {
  childs[0]->produce(codegen, context, out);
}

void FilterOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream &out) {
  codegen->generateCode(predicate_, context, out);
  parent->consume(codegen, context, out);
}

const OperatorPtr FilterOperator::copy() const { return std::make_shared<FilterOperator>(*this); }

const std::string FilterOperator::toString() const {
  std::stringstream ss;
  ss << "FILTER(" << iotdb::toString(predicate_) << ")";
  return ss.str();
}

OperatorType FilterOperator::getOperatorType() const { return FILTER_OP; }

FilterOperator::~FilterOperator() {}

bool FilterOperator::equals(const Operator &_rhs) {
  try {
    auto rhs = dynamic_cast<const iotdb::FilterOperator &>(_rhs);
    return predicate_->equals(*rhs.predicate_.get());
  }
  catch (const std::bad_cast &e) {
    return false;
  }
}

const OperatorPtr createFilterOperator(const PredicatePtr &predicate) {
  return std::make_shared<FilterOperator>(predicate);
}

} // namespace iotdb
BOOST_CLASS_EXPORT(iotdb::FilterOperator);
