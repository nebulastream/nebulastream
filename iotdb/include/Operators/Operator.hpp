#ifndef OPERATOR_OPERATOR_H
#define OPERATOR_OPERATOR_H

#include <string>
#include <memory>
#include <vector>
#include <iostream>

namespace iotdb{

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineContext;
typedef std::unique_ptr<PipelineContext> PipelineContextPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class Operator {
public:
  virtual ~Operator();
  virtual const OperatorPtr copy() = 0;
  size_t cost;
  std::vector<OperatorPtr> childs;
  virtual void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) = 0;
  virtual void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) = 0;
  virtual const std::string toString() const = 0;
};

OperatorPtr createScan(DataSourcePtr source);

OperatorPtr createSelection(DataSourcePtr source);


}

#endif // OPERATOR_OPERATOR_H
