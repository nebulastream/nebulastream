

#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/SortOperator.hpp>

namespace NES {

SortOperator::SortOperator(const Sort& sort_spec) : Operator(), sort_spec_(NES::copy(sort_spec)) {}

SortOperator::SortOperator(const SortOperator& other) : sort_spec_(NES::copy(other.sort_spec_)) {}

SortOperator& SortOperator::operator=(const SortOperator& other) {
    if (this != &other) {
        sort_spec_ = NES::copy(other.sort_spec_);
    }
    return *this;
}

void SortOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}
void SortOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}

const OperatorPtr SortOperator::copy() const {
    const OperatorPtr copy = std::make_shared<SortOperator>(*this);
    copy->setOperatorId(this->getOperatorId());
    return copy;
}

const std::string SortOperator::toString() const {
    std::stringstream ss;
    ss << "SORT(" << NES::toString(sort_spec_) << ")";
    return ss.str();
}

OperatorType SortOperator::getOperatorType() const { return SORT_OP; }

SortOperator::~SortOperator() {}

const OperatorPtr createSortOperator(const Sort& sort_spec) { return std::make_shared<SortOperator>(sort_spec); }

}// namespace NES
