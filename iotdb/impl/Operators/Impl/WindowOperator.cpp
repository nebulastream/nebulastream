

#include <iostream>
#include <sstream>
#include <vector>
#include <CodeGen/CodeGen.hpp>
#include <Operators/Impl/WindowOperator.hpp>

namespace iotdb {

WindowOperator::WindowOperator(const WindowDefinitionPtr &window_definition)
    : Operator(), window_definition(window_definition) {}

WindowOperator::WindowOperator(const WindowOperator &other) : window_definition(other.window_definition) {}

WindowOperator &WindowOperator::operator=(const WindowOperator &other) {
  if (this != &other) {
    window_definition = other.window_definition;
  }
  return *this;
}

void WindowOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream &out) {
  childs[0]->produce(codegen, context, out);

}

void WindowOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream &out) {
  codegen->generateCode(this->window_definition, context, out);
  if (parent != nullptr)
    parent->consume(codegen, context, out);
}

const OperatorPtr WindowOperator::copy() const { return std::make_shared<WindowOperator>(*this); }

const std::string WindowOperator::toString() const {
  std::stringstream ss;
  ss << "WINDOW()";
  return ss.str();
}

OperatorType WindowOperator::getOperatorType() const { return WINDOW_OP; }

WindowOperator::~WindowOperator() {}
const WindowDefinitionPtr &WindowOperator::getWindowDefinition() const {
  return window_definition;
}

const OperatorPtr createWindowOperator(const iotdb::WindowDefinitionPtr& windowDefinitionPtr) {
  return std::make_shared<WindowOperator>(windowDefinitionPtr);
}

} // namespace iotdb
