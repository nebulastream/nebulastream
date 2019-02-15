
#include <Operators/Operator.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Runtime/DataSource.hpp>
#include <sstream>

namespace iotdb {

  Operator::~Operator(){

  }

class Scan : public Operator {
public:
  Scan(DataSourcePtr src);
  Scan(const Scan&);
  void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  const OperatorPtr copy() override;
  const std::string toString() const override;
  ~Scan() override;
private:
  DataSourcePtr source;
};

Scan::Scan(DataSourcePtr src) : Operator (), source(src) {

}

Scan::Scan(const Scan& scan)
  : source(scan.source){
}

void Scan::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}
void Scan::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}

const OperatorPtr Scan::copy(){
  return std::make_shared<Scan>(*this);
}

const std::string Scan::toString() const{
  std::stringstream ss;
  ss << "SCAN(" << source->toString() << ")";
  return ss.str();
}

Scan::~Scan(){
}

//const std::string toString(const PredicatePtr& predicate){
//  return "";
//}

//class Selection : public Operator {
//public:
//  Selection(PredicatePtr _predicate) : Operator (SELECTION), predicate(_predicate) {}

//  void produce(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) override {}
//  void consume(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) override {}
//  std::string toString() const override {
//    std::stringstream ss;
//    ss << "SELECTION(" << iotdb::toString(predicate) << ")";
//    return ss.str();
//  }

//  ~Selection() override{

//  }

//private:
//  PredicatePtr predicate;
//};

OperatorPtr createScan(DataSourcePtr source) { return std::make_shared<Scan>(source); }

//OperatorPtr createSelection(PredicatePtr predicate) { return std::make_shared<Selection>(predicate); }
//}
}
