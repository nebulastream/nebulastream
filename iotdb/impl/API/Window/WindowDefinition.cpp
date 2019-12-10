#include <API/Window/WindowDefinition.hpp>
#include <API/AbstractWindowDefinition.hpp>
namespace iotdb {

WindowDefinition::WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(nullptr) {}

WindowDefinition::WindowDefinition(const AttributeFieldPtr onKey,
                                   const WindowAggregationPtr windowAggregation,
                                   const WindowTypePtr windowType)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(onKey) {

}

}