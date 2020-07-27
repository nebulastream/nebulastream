#include <assert.h>
#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/SinkOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES {

SinkOperator::SinkOperator(const DataSinkPtr sink) : Operator(), sink_(NES::copy(sink)) {}

SinkOperator::SinkOperator(const SinkOperator& other) : sink_(NES::copy(other.sink_)) {}

SinkOperator& SinkOperator::operator=(const SinkOperator& other) {
    if (this != &other) {
        sink_ = NES::copy(other.sink_);
    }
    return *this;
}

void SinkOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    assert(!getChildren().empty());
    getChildren()[0]->produce(codegen, context, out);
}

void SinkOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    codegen->generateCodeForEmit(context->getResultSchema(), context);
    /* no call to compileLiftCombine of parent, ends code generation */
}

const OperatorPtr SinkOperator::copy() const {
    const OperatorPtr copy = std::make_shared<SinkOperator>(*this);
    copy->setOperatorId(this->getOperatorId());
    return copy;
}

const std::string SinkOperator::toString() const {
    std::stringstream ss;
    ss << "SINK(" << NES::toString(sink_) << ")";
    return ss.str();
}

OperatorType SinkOperator::getOperatorType() const { return SINK_OP; }

NES::DataSinkPtr SinkOperator::getDataSinkPtr() { return sink_; }

SinkOperator::~SinkOperator() = default;

SinkOperator::SinkOperator() = default;

const OperatorPtr createSinkOperator(const DataSinkPtr sink) { return std::make_shared<SinkOperator>(sink); }

}// namespace NES
