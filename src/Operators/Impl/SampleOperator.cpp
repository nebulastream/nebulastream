#include <Operators/Impl/SampleOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <iostream>

namespace NES {

SampleOperator::SampleOperator(const std::string& udfs)
    : Operator(),
      udfs(udfs) {
}

SampleOperator::SampleOperator(const SampleOperator& other)
    : udfs(other.udfs) {
}

SampleOperator& SampleOperator::operator=(const SampleOperator& other) {
    if (this != &other) {
        udfs = other.udfs;
    }
    return *this;
}

void SampleOperator::produce(CodeGeneratorPtr codegen,
                             PipelineContextPtr context, std::ostream& out) {
    getChildren()[0]->produce(codegen, context, out);
}

void SampleOperator::consume(CodeGeneratorPtr codegen,
                             PipelineContextPtr context, std::ostream& out) {
    //  codegen->generateCode(udfs, context, out);
    getParent()->consume(codegen, context, out);
}

const OperatorPtr SampleOperator::copy() const {
    const OperatorPtr copy = std::make_shared<SampleOperator>(*this);
    copy->setOperatorId(this->getOperatorId());
    return copy;
}

const std::string SampleOperator::toString() const {
    std::stringstream ss;
    ss << "SAMPLE(" << udfs << ")";
    return ss.str();
}

OperatorType SampleOperator::getOperatorType() const {
    return SAMPLE_OP;
}

SampleOperator::~SampleOperator() {
}

bool SampleOperator::equals(const Operator& _rhs) {
    try {
        auto rhs = dynamic_cast<const NES::SampleOperator&>(_rhs);
        return udfs == rhs.udfs;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

const OperatorPtr createSampleOperator(const std::string& udfs) {
    return std::make_shared<SampleOperator>(udfs);
}

}// namespace NES
BOOST_CLASS_EXPORT(NES::SampleOperator);
