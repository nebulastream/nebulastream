


#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/MapOperator.hpp>

namespace iotdb {

MapOperator::MapOperator(const MapperPtr& mapper)
  : Operator (), mapper_(iotdb::copy(mapper))
{
}

MapOperator::MapOperator(const MapOperator& other)
  : mapper_(iotdb::copy(other.mapper_))
{
}

MapOperator& MapOperator::operator = (const MapOperator& other){
  if (this != &other){
    mapper_ = iotdb::copy(other.mapper_);
  }
  return *this;
}

void MapOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}
void MapOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}

const OperatorPtr MapOperator::copy() const{
  return std::make_shared<MapOperator>(*this);
}

const std::string MapOperator::toString() const{
  std::stringstream ss;
  ss << "MAP_UDF(" << iotdb::toString(mapper_) << ")";
  return ss.str();
}

OperatorType MapOperator::getOperatorType() const{
  return MAP_OP;
}

MapOperator::~MapOperator(){

}

const OperatorPtr createMapOperator(const MapperPtr& mapper){
  return std::make_shared<MapOperator>(mapper);
}

}

