#ifndef OPERATOR_OPERATOR_H
#define OPERATOR_OPERATOR_H

#include <string>
#include <memory>
#include <vector>
#include <iostream>
#include <API/InputQuery.hpp>

namespace iotdb{

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

enum OperatorType{SOURCE_OP,FILTER_OP,AGGREGATION_OP, SORT_OP, JOIN_OP, SET_OP, WINDOW_OP, KEYBY_OP, MAP_OP, SINK_OP};

class Operator {
public:
  virtual ~Operator();
  virtual const OperatorPtr copy() const = 0;
  size_t cost;
  std::vector<OperatorPtr> childs;
  virtual void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) = 0;
  virtual void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) = 0;
  virtual const std::string toString() const = 0;
  virtual OperatorType getOperatorType() const = 0;
};

const OperatorPtr createAggregationOperator(const AggregationSpec& aggr_spec);
const OperatorPtr createFilterOperator(const PredicatePtr& predicate);
const OperatorPtr createJoinOperator(const JoinPredicatePtr& join_spec);
const OperatorPtr createKeyByOperator(const Attributes& keyby_spec);
const OperatorPtr createMapOperator(const MapperPtr& mapper);
const OperatorPtr createSinkOperator(const DataSinkPtr& sink);
const OperatorPtr createSortOperator(const Sort& sort_spec);
const OperatorPtr createSourceOperator(const DataSourcePtr& source);
const OperatorPtr createWindowOperator(const WindowPtr& window_spec);


}

#endif // OPERATOR_OPERATOR_H
