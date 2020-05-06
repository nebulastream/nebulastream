

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace NES {

class WindowOperator : public Operator {
  public:
    WindowOperator(const WindowDefinitionPtr& window_definition);

    WindowOperator(const WindowOperator& other);

    WindowOperator& operator=(const WindowOperator& other);

    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

    const OperatorPtr copy() const override;

    const std::string toString() const override;

    OperatorType getOperatorType() const override;

    ~WindowOperator() override;

    const WindowDefinitionPtr& getWindowDefinition() const;

  private:
    WindowDefinitionPtr window_definition;
};

}// namespace NES
