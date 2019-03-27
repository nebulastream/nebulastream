

#include <assert.h>
#include <iostream>
#include <sstream>
#include <vector>

#include <CodeGen/CodeGen.hpp>
#include <Operators/Impl/SinkOperator.hpp>

namespace iotdb {

SinkOperator::SinkOperator(const DataSinkPtr& sink) : Operator(), sink_(iotdb::copy(sink)) {}

SinkOperator::SinkOperator(const SinkOperator& other) : sink_(iotdb::copy(other.sink_)) {}

SinkOperator& SinkOperator::operator=(const SinkOperator& other)
{
    if (this != &other) {
        sink_ = iotdb::copy(other.sink_);
    }
    return *this;
}

void SinkOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out)
{
    assert(!childs.empty());
    childs[0]->produce(codegen, context, out);
}

void SinkOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out)
{
    codegen->generateCode(sink_, context, out);
    /* no call to consume of parent, ends code generation */
}

const OperatorPtr SinkOperator::copy() const { return std::make_shared<SinkOperator>(*this); }

const std::string SinkOperator::toString() const
{
    std::stringstream ss;
    ss << "SINK(" << iotdb::toString(sink_) << ")";
    return ss.str();
}

OperatorType SinkOperator::getOperatorType() const { return SINK_OP; }

SinkOperator::~SinkOperator() {}

const OperatorPtr createSinkOperator(const DataSinkPtr& sink) { return std::make_shared<SinkOperator>(sink); }

} // namespace iotdb
