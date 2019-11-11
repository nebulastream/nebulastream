#include <API/Window/WindowDefinition.hpp>

namespace iotdb {

WindowDefinition::WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType)
    : windowAggregation(windowAggregation), windowType(windowType) {}

}