

#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/AggregationOperator.hpp>

namespace NES {

AggregationOperator::AggregationOperator(const AggregationSpec& aggr_spec)
    : Operator(), aggr_spec_(NES::copy(aggr_spec)) {
}

AggregationOperator::AggregationOperator(const AggregationOperator& other) : aggr_spec_(NES::copy(other.aggr_spec_)) {
}

AggregationOperator& AggregationOperator::operator=(const AggregationOperator& other) {
    if (this != &other) {
        aggr_spec_ = NES::copy(other.aggr_spec_);
    }
    return *this;
}

void AggregationOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}
void AggregationOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}

const OperatorPtr AggregationOperator::copy() const {
    const OperatorPtr copy = std::make_shared<AggregationOperator>(*this);
    copy->setOperatorId(this->getOperatorId());
    return copy;
}

const std::string AggregationOperator::toString() const {
    std::stringstream ss;
    ss << "AGGREGATE(" << NES::toString(aggr_spec_) << ")";
    return ss.str();
}

OperatorType AggregationOperator::getOperatorType() const { return AGGREGATION_OP; }

AggregationOperator::~AggregationOperator() {}

const OperatorPtr createAggregationOperator(const AggregationSpec& aggr_spec) {
    return std::make_shared<AggregationOperator>(aggr_spec);
}

}// namespace NES
