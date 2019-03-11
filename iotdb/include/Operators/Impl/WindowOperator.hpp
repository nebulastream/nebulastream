

#pragma once

#include <string>
#include <memory>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace iotdb {

class WindowOperator : public Operator {
public:
  WindowOperator(const WindowPtr& window_spec);
  WindowOperator(const WindowOperator& other);
  WindowOperator& operator = (const WindowOperator& other);
  void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  const OperatorPtr copy() const override;
  const std::string toString() const override;
  OperatorType getOperatorType() const override;
  ~WindowOperator() override;
private:
  WindowPtr window_spec_;
};

}

