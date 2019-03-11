


#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/JoinOperator.hpp>

namespace iotdb {

JoinOperator::JoinOperator(const JoinPredicatePtr& join_spec)
  : Operator (), join_spec_(iotdb::copy(join_spec))
{
}

JoinOperator::JoinOperator(const JoinOperator& other)
  : join_spec_(iotdb::copy(other.join_spec_))
{
}

JoinOperator& JoinOperator::operator = (const JoinOperator& other){
  if (this != &other){
    join_spec_ = iotdb::copy(other.join_spec_);
  }
  return *this;
}

void JoinOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}
void JoinOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}

const OperatorPtr JoinOperator::copy() const{
  return std::make_shared<JoinOperator>(*this);
}

const std::string JoinOperator::toString() const{
  std::stringstream ss;
  ss << "JOIN(" << iotdb::toString(join_spec_) << ")";
  return ss.str();
}

OperatorType JoinOperator::getOperatorType() const{
  return JOIN_OP;
}

JoinOperator::~JoinOperator(){

}

const OperatorPtr createJoinOperator(const JoinPredicatePtr& join_spec){
  return std::make_shared<JoinOperator>(join_spec);
}

}

