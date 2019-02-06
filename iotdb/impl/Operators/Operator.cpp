
//#include <Operators/Operator.hpp>

//#include <Runtime/DataSource.hpp>
//#include <sstream>

//namespace iotdb {

//  Operator::Operator(OperatorType op_type) : type(op_type), childs(){

//  }

//  Operator::~Operator(){

//  }



//class Scan : public Operator {
//public:
//  Scan(DataSourcePtr src) : Operator (SCAN), source(src) {}

//  void produce(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) override {}
//  void consume(CodeGeneratorPtr codegen, QueryExecutionPlanPtr qep) override {}
//  std::string toString() const override {
//    std::stringstream ss;
//    ss << "SCAN(" << source->toString() << ")";
//    return ss.str();
//  }

//  ~Scan() override{

//  }

//private:
//  DataSourcePtr source;
//};

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

//OperatorPtr createScan(DataSourcePtr source) { return std::make_shared<Scan>(source); }

//OperatorPtr createSelection(PredicatePtr predicate) { return std::make_shared<Selection>(predicate); }
//}

