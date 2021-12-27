#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedSliceMergingOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalKeyedSliceMergingOperator::PhysicalKeyedSliceMergingOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractScanOperator(),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler) {}
std::string PhysicalKeyedSliceMergingOperator::toString() const {
    return "PhysicalKeyedSliceMergingOperator";
}

OperatorNodePtr PhysicalKeyedSliceMergingOperator::copy() {
    return create(inputSchema, outputSchema, keyedEventTimeWindowHandler);
}
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES