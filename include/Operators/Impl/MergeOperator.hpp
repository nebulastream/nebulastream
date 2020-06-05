#pragma once

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>
#include <memory>
#include <string>

namespace NES {

/**
 * Merges two input streams and produce one output stream.
 */
class MergeOperator : public Operator {
  public:
    MergeOperator(SchemaPtr sch);
    MergeOperator(const MergeOperator& other);
    MergeOperator& operator=(const MergeOperator& other);

    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    /**
     * @brief
     * @param codegen generating the emit function.
     * @param context
     * @param out
     */
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~MergeOperator() override;

  private:
    SchemaPtr schemaPtr;
};

}// namespace NES
