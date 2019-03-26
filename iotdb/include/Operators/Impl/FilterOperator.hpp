

#pragma once

#include <string>
#include <memory>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace iotdb {

class FilterOperator : public Operator {
public:
  FilterOperator(const PredicatePtr& predicate);
  FilterOperator(const FilterOperator& other);
  FilterOperator& operator = (const FilterOperator& other);
  void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  const OperatorPtr copy() const override;
  const std::string toString() const override;
  OperatorType getOperatorType() const override;
  ~FilterOperator() override;
private:
  PredicatePtr predicate_;
};

}

