


#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/SourceOperator.hpp>
#include <CodeGen/CodeGen.hpp>

namespace iotdb {

SourceOperator::SourceOperator(const DataSourcePtr& source)
  : Operator (), source_(source)
{
}

SourceOperator::SourceOperator(const SourceOperator& other)
  : source_(other.source_)
{
}

SourceOperator& SourceOperator::operator = (const SourceOperator& other){
  if (this != &other){
    source_ = other.source_;
  }
  return *this;
}

void SourceOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){
    this->consume(codegen,context,out);
}

void SourceOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){
  codegen->generateCode(source_, context, out);
  //parent->consume(codegen,context,out);
}

const OperatorPtr SourceOperator::copy() const{
  return std::make_shared<SourceOperator>(*this);
}

const std::string SourceOperator::toString() const{
  std::stringstream ss;
  ss << "SOURCE(" << iotdb::toString(source_) << ")";
  return ss.str();
}

OperatorType SourceOperator::getOperatorType() const{
  return SOURCE_OP;
}

SourceOperator::~SourceOperator(){

}

const OperatorPtr createSourceOperator(const DataSourcePtr& source){
  return std::make_shared<SourceOperator>(source);
}

}

