
#include <Operators/Operator.hpp>

#include <Runtime/DataSource.hpp>

namespace iotdb {

  Operator::Operator(OperatorType op_type) : type(op_type), childs(){

  }

  Operator::~Operator(){

  }

class Scan : public Operator {
public:
  Scan(DataSourcePtr src) : Operator (SCAN), source(src) {}

  void produce(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) override {}
  void consume(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) override {}
  std::string toString() const override { return std::string("SCAN"); }

  ~Scan() override{

  }

private:
  DataSourcePtr source;
};



OperatorPtr createScan(DataSourcePtr source) { return std::make_unique<Scan>(source); }

OperatorPtr createSelection(PredicatePtr predicate) { return OperatorPtr(); }
}
