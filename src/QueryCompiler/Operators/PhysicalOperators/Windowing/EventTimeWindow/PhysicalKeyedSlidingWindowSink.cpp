#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedSlidingWindowSink.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalKeyedSlidingWindowSink::PhysicalKeyedSlidingWindowSink(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractScanOperator(),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler) {}

std::string PhysicalKeyedSlidingWindowSink::toString() const { return "PhysicalKeyedSlidingWindowSink"; }

OperatorNodePtr PhysicalKeyedSlidingWindowSink::copy() { return create(inputSchema, outputSchema, keyedEventTimeWindowHandler); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES