


#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/KeyByOperator.hpp>

namespace iotdb {

KeyByOperator::KeyByOperator(const Attributes& keyby_spec)
  : Operator (), keyby_spec_(iotdb::copy(keyby_spec))
{
}

KeyByOperator::KeyByOperator(const KeyByOperator& other)
  : keyby_spec_(iotdb::copy(other.keyby_spec_))
{
}

KeyByOperator& KeyByOperator::operator = (const KeyByOperator& other){
  if (this != &other){
    keyby_spec_ = iotdb::copy(other.keyby_spec_);
  }
  return *this;
}

void KeyByOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}
void KeyByOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}

const OperatorPtr KeyByOperator::copy() const{
  return std::make_shared<KeyByOperator>(*this);
}

const std::string KeyByOperator::toString() const{
  std::stringstream ss;
  ss << "KEYBY(" << iotdb::toString(keyby_spec_) << ")";
  return ss.str();
}

OperatorType KeyByOperator::getOperatorType() const{
  return KEYBY_OP;
}

KeyByOperator::~KeyByOperator(){

}

const OperatorPtr createKeyByOperator(const Attributes& keyby_spec){
  return std::make_shared<KeyByOperator>(keyby_spec);
}

}

