#pragma once

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>
#include <memory>
#include <string>

namespace NES {

/**
 * @brief Merges Operator to merge two input streams to one output stream.
 * This operator assumes that the output of both child operators is the same.
 */
class MergeOperator : public Operator {
  public:
    /**
     * @brief Creates a new merge operator, which receives the output schema as a parameter.
     * @param outputSchema
     */
    MergeOperator(SchemaPtr outputSchema);
    MergeOperator(const MergeOperator& other);
    MergeOperator& operator=(const MergeOperator& other);

    /**
     * @brief Produce function for the code generation.
     * @param codegen
     * @param context
     * @param out
     */
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

    /**
     * @brief Consume function for the code generation.
     * @param codegen
     * @param context
     * @param out
     */
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~MergeOperator() override = default;

  private:
    SchemaPtr schemaPtr;
};

}// namespace NES
