

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace iotdb {

class SinkOperator : public Operator {
  public:
    SinkOperator(const DataSinkPtr& sink);
    SinkOperator(const SinkOperator& other);
    SinkOperator& operator=(const SinkOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~SinkOperator() override;

  private:
    DataSinkPtr sink_;
};

} // namespace iotdb
