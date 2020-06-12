#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace NES {

class JoinOperator : public Operator {
  public:
    JoinOperator(const JoinPredicatePtr join_spec);
    JoinOperator(const JoinOperator& other);
    JoinOperator& operator=(const JoinOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~JoinOperator() override;

  private:
    JoinPredicatePtr join_spec_;
};

}// namespace NES
