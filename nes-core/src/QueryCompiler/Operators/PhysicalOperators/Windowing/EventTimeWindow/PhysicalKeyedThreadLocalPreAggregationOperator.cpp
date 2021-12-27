#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalKeyedThreadLocalPreAggregationOperator::PhysicalKeyedThreadLocalPreAggregationOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler) {}
std::string PhysicalKeyedThreadLocalPreAggregationOperator::toString() const {
    return "PhysicalKeyedThreadLocalPreAggregationOperator";
}

OperatorNodePtr PhysicalKeyedThreadLocalPreAggregationOperator::copy() {
    return create(inputSchema, outputSchema, keyedEventTimeWindowHandler);
}
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES