
#include <Interpreter/Operators/ExecuteOperator.hpp>
#include <Interpreter/Operators/Operator.hpp>
namespace NES::Interpreter {

void Operator::open(ExecutionContext& executionCtx, RecordBuffer& rb) const {
    if (hasChildren()) {
        child->open(executionCtx, rb);
    }
}

void Operator::close(ExecutionContext& executionCtx, RecordBuffer& rb) const {
    if (hasChildren()) {
        child->close(executionCtx, rb);
    }
}

bool Operator::hasChildren() const { return child != nullptr; }

void Operator::setChild(ExecuteOperatorPtr child) { this->child = std::move(child); }

Operator::~Operator() {}

}// namespace NES::Interpreter