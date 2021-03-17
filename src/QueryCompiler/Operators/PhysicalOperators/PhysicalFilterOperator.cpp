
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

PhysicalFilterOperator::
    PhysicalFilterOperator(OperatorId id, ExpressionNodePtr predicate):
              OperatorNode(id), PhysicalUnaryOperator(id), AbstractFilterOperator(predicate) {}

PhysicalOperatorPtr PhysicalFilterOperator::create(OperatorId id, ExpressionNodePtr expression) {
    return std::make_shared<PhysicalFilterOperator>(id, expression);
}

PhysicalOperatorPtr PhysicalFilterOperator::create(ExpressionNodePtr expression) {
    return create(UtilityFunctions::getNextOperatorId(), expression);
}

const std::string PhysicalFilterOperator::toString() const { return "PhysicalFilterOperator"; }

OperatorNodePtr PhysicalFilterOperator::copy() { return create(id, getPredicate()); }

}
}
}