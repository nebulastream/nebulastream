

#pragma once

#include <string>
#include <memory>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace iotdb {

class MapOperator : public Operator {
public:
  MapOperator(const MapperPtr& mapper);
  MapOperator(const MapOperator& other);
  MapOperator& operator = (const MapOperator& other);
  void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  const OperatorPtr copy() const override;
  const std::string toString() const override;
  OperatorType getOperatorType() const override;
  ~MapOperator() override;
private:
  MapperPtr mapper_;
};

}

