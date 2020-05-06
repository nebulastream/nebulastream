

#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/KeyByOperator.hpp>

namespace NES {

KeyByOperator::KeyByOperator(const Attributes& keyby_spec) : Operator(), keyby_spec_(NES::copy(keyby_spec)) {}

KeyByOperator::KeyByOperator(const KeyByOperator& other) : keyby_spec_(NES::copy(other.keyby_spec_)) {}

KeyByOperator& KeyByOperator::operator=(const KeyByOperator& other) {
    if (this != &other) {
        keyby_spec_ = NES::copy(other.keyby_spec_);
    }
    return *this;
}

void KeyByOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}
void KeyByOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}

const OperatorPtr KeyByOperator::copy() const {
    const OperatorPtr copy = std::make_shared<KeyByOperator>(*this);
    copy->setOperatorId(this->getOperatorId());
    return copy;
}

const std::string KeyByOperator::toString() const {
    std::stringstream ss;
    ss << "KEYBY(" << NES::toString(keyby_spec_) << ")";
    return ss.str();
}

OperatorType KeyByOperator::getOperatorType() const { return KEYBY_OP; }

KeyByOperator::~KeyByOperator() {}

const OperatorPtr createKeyByOperator(const Attributes& keyby_spec) {
    return std::make_shared<KeyByOperator>(keyby_spec);
}

}// namespace NES
