

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace NES {

class SortOperator : public Operator {
  public:
    SortOperator(const Sort& sort_spec);
    SortOperator(const SortOperator& other);
    SortOperator& operator=(const SortOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~SortOperator() override;

  private:
    Sort sort_spec_;
};

}// namespace NES
