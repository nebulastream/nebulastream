

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <API/UserAPIExpression.hpp>
#include <Operators/Operator.hpp>

namespace NES {

class MapOperator : public Operator {
  public:
    MapOperator(AttributeFieldPtr field, UserAPIExpressionPtr ptr);
    MapOperator(const MapOperator& other);
    MapOperator& operator=(const MapOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~MapOperator() override;
    const UserAPIExpressionPtr& getPredicate() const;
    const AttributeFieldPtr& getField() const;

  private:
    UserAPIExpressionPtr predicate_;
    AttributeFieldPtr field_;
};

}// namespace NES
