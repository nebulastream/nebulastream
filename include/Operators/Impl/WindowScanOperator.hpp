

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace NES {

/**
 * @brief The window scan operator receives the window aggregation results as an input for the next pipeline.
 */
class WindowScanOperator : public Operator {
  public:
    WindowScanOperator(SchemaPtr sch);
    WindowScanOperator();

    WindowScanOperator(const WindowScanOperator& other);

    WindowScanOperator& operator=(const WindowScanOperator& other);

    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

    const OperatorPtr copy() const override;

    const std::string toString() const override;

    OperatorType getOperatorType() const override;

    ~WindowScanOperator() override;

  private:
    SchemaPtr schemaPtr;
};

}// namespace NES
