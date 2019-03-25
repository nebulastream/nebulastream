

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace iotdb {

class SourceOperator : public Operator {
  public:
    SourceOperator(const DataSourcePtr& source);
    SourceOperator(const SourceOperator& other);
    SourceOperator& operator=(const SourceOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~SourceOperator() override;

  private:
    DataSourcePtr source_;
};

} // namespace iotdb
