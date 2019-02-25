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
typedef std::unique_ptr<PipelineContext> PipelineContextPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

enum OperatorType{SCAN_OP,FILTER_OP,AGGREGATION_OP, SORT_OP, JOIN_OP, SET_OP, WINDOW_OP, KEYBY_OP, MAP_OP, OUTPUT_OPERATOR};

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

const OperatorPtr createScanOperator(DataSourcePtr source);
const OperatorPtr createSelectionOperator(const PredicatePtr& predicate);
const OperatorPtr createGroupedAggregationOperator(const Attributes& grouping_fields, const AggregationPtr& aggr_spec);
const OperatorPtr createOrderByOperator(const Sort& fields);
const OperatorPtr createJoinOperator(const InputQuery& sub_query, const JoinPredicatePtr& joinPred);

const OperatorPtr createWindowOperator(const WindowPtr& window);
const OperatorPtr createKeyByOperator(const Attributes& fields);
const OperatorPtr createMapOperator(const MapperPtr& mapper);

const OperatorPtr createWriteFileOperator(const std::string& file_name);
const OperatorPtr createPrintOperator(std::ostream& out);

}

#endif // OPERATOR_OPERATOR_H
