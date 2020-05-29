#include <Operators/Impl/FilterOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <iostream>

namespace NES {

FilterOperator::FilterOperator(const PredicatePtr& predicate) : Operator(), predicate(NES::copy(predicate)) {}

FilterOperator::FilterOperator(const FilterOperator& other) : predicate(NES::copy(other.predicate)) {}

FilterOperator& FilterOperator::operator=(const FilterOperator& other) {
    if (this != &other) {
        predicate = NES::copy(other.predicate);
    }
    return *this;
}

void FilterOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    getChildren()[0]->produce(codegen, context, out);
}

void FilterOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    codegen->generateCode(predicate, context, out);
    getParent()->consume(codegen, context, out);
}

const OperatorPtr FilterOperator::copy() const {
    const OperatorPtr copy = std::make_shared<FilterOperator>(*this);
    copy->setOperatorId(this->getOperatorId());
    return copy;
}

const std::string FilterOperator::toString() const {
    std::stringstream ss;
    ss << "FILTER(" << NES::toString(predicate) << ")";
    return ss.str();
}

PredicatePtr FilterOperator::getPredicate() {
    return predicate;
}

OperatorType FilterOperator::getOperatorType() const { return FILTER_OP; }

FilterOperator::~FilterOperator() {}

bool FilterOperator::equals(const Operator& _rhs) {
    try {
        auto rhs = dynamic_cast<const NES::FilterOperator&>(_rhs);
        return predicate->equals(*rhs.predicate.get());
    } catch (const std::bad_cast& e) {
        return false;
    }
}

const OperatorPtr createFilterOperator(const PredicatePtr predicate) {
    return std::make_shared<FilterOperator>(predicate);
}

}// namespace NES
BOOST_CLASS_EXPORT(NES::FilterOperator);
