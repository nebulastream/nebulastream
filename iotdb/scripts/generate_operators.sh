
#!bin/bash



generate_class () {

mkdir -p operators_hpp
mkdir -p operators_cpp

CLASS_NAME=$1
PARAMETER_TYPE=$2
OPERATOR_NAME=$3
VAR_NAME="$4"
VAR_NAME_INTERN="$VAR_NAME""_"
OPERATOR_ENUM_NAME=$5

echo "

#pragma once

#include <string>
#include <memory>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace iotdb {

class $CLASS_NAME : public Operator {
public:
  $CLASS_NAME(const $PARAMETER_TYPE& $VAR_NAME);
  $CLASS_NAME(const $CLASS_NAME& other);
  $CLASS_NAME& operator = (const $CLASS_NAME& other);
  void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
  const OperatorPtr copy() const override;
  const std::string toString() const override;
  OperatorType getOperatorType() const override;
  ~$CLASS_NAME() override;
private:
  $PARAMETER_TYPE $VAR_NAME_INTERN;
};

}
" > "operators_hpp/$CLASS_NAME.hpp"

echo "


#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/$CLASS_NAME.hpp>

namespace iotdb {

$CLASS_NAME::$CLASS_NAME(const $PARAMETER_TYPE& $VAR_NAME)
  : Operator (), $VAR_NAME_INTERN(iotdb::copy($VAR_NAME))
{
}

$CLASS_NAME::$CLASS_NAME(const $CLASS_NAME& other)
  : $VAR_NAME_INTERN(iotdb::copy(other.$VAR_NAME_INTERN))
{
}

$CLASS_NAME& $CLASS_NAME::operator = (const $CLASS_NAME& other){
  if (this != &other){
    $VAR_NAME_INTERN = iotdb::copy(other.$VAR_NAME_INTERN);
  }
  return *this;
}

void $CLASS_NAME::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}
void $CLASS_NAME::compileLiftCombine(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out){

}

const OperatorPtr $CLASS_NAME::copy() const{
  return std::make_shared<$CLASS_NAME>(*this);
}

const std::string $CLASS_NAME::toString() const{
  std::stringstream ss;
  ss << \"$OPERATOR_NAME(\" << iotdb::toString($VAR_NAME_INTERN) << \")\";
  return ss.str();
}

OperatorType $CLASS_NAME::getOperatorType() const{
  return $OPERATOR_ENUM_NAME;
}

$CLASS_NAME::~$CLASS_NAME(){

}

const OperatorPtr create$CLASS_NAME(const $PARAMETER_TYPE& $VAR_NAME){
  return std::make_shared<$CLASS_NAME>($VAR_NAME);
}

}
" > "operators_cpp/$CLASS_NAME.cpp"

}

generate_class "SourceOperator" "DataSourcePtr" "SOURCE" "source" "SOURCE_OP"

generate_class "SinkOperator" "DataSinkPtr" "SINK" "sink" "SINK_OP"

generate_class "FilterOperator" "PredicatePtr" "FILTER" "predicate" "FILTER_OP"

generate_class "SortOperator" "Sort" "SORT" "sort_spec" "SORT_OP"

generate_class "WindowOperator" "WindowPtr" "WINDOW" "window_spec" "WINDOW_OP"

generate_class "KeyByOperator" "Attributes" "KEYBY" "keyby_spec" "KEYBY_OP"

generate_class "MapOperator" "MapperPtr" "MAP_UDF" "mapper" "MAP_OP"

generate_class "AggregationOperator" "AggregationSpec" "AGGREGATE" "aggr_spec" "AGGREGATION_OP"

generate_class "JoinOperator" "JoinPredicatePtr" "JOIN" "join_spec" "JOIN_OP"

cp operators_hpp/* /home/sebastian/Software/iotdb/iotdb/include/Operators/Impl/
cp operators_cpp/* /home/sebastian/Software/iotdb/iotdb/impl/Operators/Impl/

