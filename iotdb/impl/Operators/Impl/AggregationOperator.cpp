

#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/AggregationOperator.hpp>

namespace iotdb {

AggregationOperator::AggregationOperator(const AggregationSpec& aggr_spec)
    : Operator(), aggr_spec_(iotdb::copy(aggr_spec))
{
}

AggregationOperator::AggregationOperator(const AggregationOperator& other) : aggr_spec_(iotdb::copy(other.aggr_spec_))
{
}

AggregationOperator& AggregationOperator::operator=(const AggregationOperator& other)
{
    if (this != &other) {
        aggr_spec_ = iotdb::copy(other.aggr_spec_);
    }
    return *this;
}

void AggregationOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}
void AggregationOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}

const OperatorPtr AggregationOperator::copy() const { return std::make_shared<AggregationOperator>(*this); }

const std::string AggregationOperator::toString() const
{
    std::stringstream ss;
    ss << "AGGREGATE(" << iotdb::toString(aggr_spec_) << ")";
    return ss.str();
}

OperatorType AggregationOperator::getOperatorType() const { return AGGREGATION_OP; }

AggregationOperator::~AggregationOperator() {}

const OperatorPtr createAggregationOperator(const AggregationSpec& aggr_spec)
{
    return std::make_shared<AggregationOperator>(aggr_spec);
}

} // namespace iotdb
