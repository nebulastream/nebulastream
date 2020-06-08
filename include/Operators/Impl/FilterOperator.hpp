#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace NES {

class FilterOperator : public Operator {
  public:
    FilterOperator() = default;

    FilterOperator(const PredicatePtr& predicate);
    FilterOperator(const FilterOperator& other);
    FilterOperator& operator=(const FilterOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    virtual bool equals(const Operator& _rhs) override;
    ~FilterOperator() override;
    PredicatePtr getPredicate();

  private:
    PredicatePtr predicate;
};

}// namespace NES
