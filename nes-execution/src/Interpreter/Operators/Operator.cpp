
#include <Interpreter/Operators/Operator.hpp>
namespace NES::Interpreter {

void Operator::setChild(ExecuteOperatorPtr child) { this->child = std::move(child); }

Operator::~Operator() {}


}