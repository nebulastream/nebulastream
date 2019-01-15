
#pragma once
#include <memory>
#include <vector>

namespace iotdb {

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class Operator;
/** \brief we need a shared ptr, because we have a query graph,
 * and we may only delete an operator tree if
 * all references to this tree where deleted */
typedef std::shared_ptr<Operator> OperatorPtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class QueryExecutionPlan;
typedef std::shared_ptr<QueryExecutionPlan> QueryExecutionPlanPtr;

enum OperatorType { SCAN, SELECTION, PROJECTION, JOIN, AGGREGATION, CUSTOM };

/** \brief stores physical properties of an operator (e.g., hash join/nested loop join, or the node the operator should be executed on) */
class PhysicalProperties;
typedef std::unique_ptr<PhysicalProperties> PhysicalPropertiesPtr;

class Operator {
public:
  virtual void produce(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) = 0;
  virtual void consume(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) = 0;
  virtual std::string toString() const = 0;
  virtual ~Operator();
  template<typename Functor>
  void traverse(Functor& f);
protected:
  Operator(OperatorType);
private:
  OperatorType type;
public:
  std::vector<OperatorPtr> childs;
};

template<typename Functor>
void Operator::traverse(Functor& f){
  for(auto child : childs){
      if(child)
        child->traverse(f);
  }
}

OperatorPtr createScan(DataSourcePtr source);
OperatorPtr createSelection(PredicatePtr predicate);
}
