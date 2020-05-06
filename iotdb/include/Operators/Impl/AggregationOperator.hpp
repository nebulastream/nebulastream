

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace NES {

class AggregationOperator : public Operator {
  public:
    AggregationOperator(const AggregationSpec& aggr_spec);
    AggregationOperator(const AggregationOperator& other);
    AggregationOperator& operator=(const AggregationOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~AggregationOperator() override;

  private:
    AggregationSpec aggr_spec_;
};

}// namespace NES
