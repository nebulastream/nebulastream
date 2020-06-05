#pragma once

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>
#include <memory>
#include <string>

namespace NES {

/**
 * merge two input streams to one output stream
 */
class MergeOperator : public Operator {
  public:
    MergeOperator(SchemaPtr sch);
    MergeOperator(const MergeOperator& other);
    MergeOperator& operator=(const MergeOperator& other);

    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~MergeOperator() override;

  private:
    SchemaPtr schemaPtr;
};

}// namespace NES
