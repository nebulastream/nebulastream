#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedGlobalSliceStoreAppendOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalKeyedGlobalSliceStoreAppendOperator::PhysicalKeyedGlobalSliceStoreAppendOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::shared_ptr<Windowing::Experimental::KeyedEventTimeWindowHandler> keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractEmitOperator(),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler) {}

std::string PhysicalKeyedGlobalSliceStoreAppendOperator::toString() const {
    return "PhysicalKeyedGlobalSliceStoreAppendOperator";
}

OperatorNodePtr PhysicalKeyedGlobalSliceStoreAppendOperator::copy() {
    return create(inputSchema, outputSchema, keyedEventTimeWindowHandler);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES