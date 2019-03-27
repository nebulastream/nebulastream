

#include <iostream>
#include <sstream>
#include <vector>

#include <Operators/Impl/WindowOperator.hpp>

namespace iotdb {

WindowOperator::WindowOperator(const WindowPtr& window_spec) : Operator(), window_spec_(iotdb::copy(window_spec)) {}

WindowOperator::WindowOperator(const WindowOperator& other) : window_spec_(iotdb::copy(other.window_spec_)) {}

WindowOperator& WindowOperator::operator=(const WindowOperator& other)
{
    if (this != &other) {
        window_spec_ = iotdb::copy(other.window_spec_);
    }
    return *this;
}

void WindowOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}
void WindowOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {}

const OperatorPtr WindowOperator::copy() const { return std::make_shared<WindowOperator>(*this); }

const std::string WindowOperator::toString() const
{
    std::stringstream ss;
    ss << "WINDOW(" << iotdb::toString(window_spec_) << ")";
    return ss.str();
}

OperatorType WindowOperator::getOperatorType() const { return WINDOW_OP; }

WindowOperator::~WindowOperator() {}

const OperatorPtr createWindowOperator(const WindowPtr& window_spec)
{
    return std::make_shared<WindowOperator>(window_spec);
}

} // namespace iotdb
