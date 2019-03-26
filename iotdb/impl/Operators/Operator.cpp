
#include <Operators/Operator.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Runtime/DataSource.hpp>
#include <sstream>
#include <Util/ErrorHandling.hpp>

namespace iotdb {

  Operator::~Operator(){

  }

//class Scan : public Operator {
//public:
//  Scan(DataSourcePtr src);
//  Scan(const Scan&);
//  void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
//  void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
//  const OperatorPtr copy() const override;
//  const std::string toString() const override;
//  OperatorType getOperatorType() const override;
//  ~Scan() override;
//private:
//  DataSourcePtr source;
//};

//Scan::Scan(DataSourcePtr src) : Operator (), source(src) {

//}

//Scan::Scan(const Scan& scan)
//  : source(scan.source){
//}

//void Scan::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

//}
//void Scan::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

//}

//const OperatorPtr Scan::copy() const{
//  return std::make_shared<Scan>(*this);
//}

//const std::string Scan::toString() const{
//  std::stringstream ss;
//  ss << "SCAN(" << source->toString() << ")";
//  return ss.str();
//}

//OperatorType Scan::getOperatorType() const{
//  return SCAN_OP;
//}

//Scan::~Scan(){
//}

//class Selection : public Operator {
//public:
//  Selection(PredicatePtr _predicate);
//  Selection(const Selection& other);
//  Selection& operator = (const Selection& other);
//  void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
//  void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
//  const OperatorPtr copy() const override;
//  const std::string toString() const override;
//  OperatorType getOperatorType() const override;
//  ~Selection() override;
//private:
//  PredicatePtr predicate;
//};

//Selection::Selection(PredicatePtr _predicate)
//  : Operator (), predicate(_predicate)
//{
//}

//Selection::Selection(const Selection& other)
//  : predicate(other.predicate)
//{
//}

//Selection& Selection::operator = (const Selection& other){
//  if (this != &other){
//    predicate = other.predicate;
//  }
//  return *this;
//}

//void Selection::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

//}
//void Selection::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

//}

//const OperatorPtr Selection::copy() const{
//  return std::make_shared<Selection>(*this);
//}

//const std::string Selection::toString() const{
//  std::stringstream ss;
//  ss << "SELECTION( *** )";
////  ss << "SELECTION(" << iotdb::toString(predicate) << ")";
//  return ss.str();
//}

//OperatorType Selection::getOperatorType() const{
//  return FILTER_OP;
//}

//Selection::~Selection(){

//}

///** \todo create the remaining operators here!
// *
// */

//const OperatorPtr createScanOperator(DataSourcePtr source) {
//  return std::make_shared<Scan>(source);
//}

//const OperatorPtr createSelectionOperator(const PredicatePtr& predicate){
//  return std::make_shared<Selection>(predicate);
//}
//const OperatorPtr createGroupedAggregationOperator(const Attributes& grouping_fields, const AggregationPtr& aggr_spec){
//  IOTDB_FATAL_ERROR("Called Unimplemented Method!");
//  return OperatorPtr();
//}
//const OperatorPtr createOrderByOperator(const Sort& fields){
//  IOTDB_FATAL_ERROR("Called Unimplemented Method!");
//  return OperatorPtr();
//}
//const OperatorPtr createJoinOperator(const InputQuery& sub_query, const JoinPredicatePtr& joinPred){
//  IOTDB_FATAL_ERROR("Called Unimplemented Method!");
//  return OperatorPtr();
//}

//const OperatorPtr createWindowOperator(const WindowPtr& window){
//  IOTDB_FATAL_ERROR("Called Unimplemented Method!");
//  return OperatorPtr();
//}

//const OperatorPtr createKeyByOperator(const Attributes& fields){
//  IOTDB_FATAL_ERROR("Called Unimplemented Method!");
//  return OperatorPtr();
//}

//const OperatorPtr createMapOperator(const MapperPtr& mapper){
//  IOTDB_FATAL_ERROR("Called Unimplemented Method!");
//  return OperatorPtr();
//}

//const OperatorPtr createWriteFileOperator(const std::string& file_name){
//  IOTDB_FATAL_ERROR("Called Unimplemented Method!");
//  return OperatorPtr();
//}

//const OperatorPtr createPrintOperator(std::ostream& out){
//  IOTDB_FATAL_ERROR("Called Unimplemented Method!");
//  return OperatorPtr();
//}

}
