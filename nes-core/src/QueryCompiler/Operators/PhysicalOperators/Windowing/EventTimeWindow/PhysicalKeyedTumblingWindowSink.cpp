#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedTumblingWindowSink.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalKeyedTumblingWindowSink::PhysicalKeyedTumblingWindowSink(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler) {}

std::string PhysicalKeyedTumblingWindowSink::toString() const { return "PhysicalKeyedTumblingWindowSink"; }

OperatorNodePtr PhysicalKeyedTumblingWindowSink::copy() { return create(inputSchema, outputSchema, keyedEventTimeWindowHandler); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES