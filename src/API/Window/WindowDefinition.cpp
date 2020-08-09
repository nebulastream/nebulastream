#include <API/AbstractWindowDefinition.hpp>
#include <API/Window/WindowDefinition.hpp>
namespace NES {

WindowDefinition::WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(nullptr) {}

WindowDefinition::WindowDefinition(const AttributeFieldPtr onKey,
                                   const WindowAggregationPtr windowAggregation,
                                   const WindowTypePtr windowType)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(onKey) {
}

bool WindowDefinition::isKeyed() {
    return onKey != nullptr;
}

WindowDefinitionPtr createWindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType) {
    return std::make_shared<WindowDefinition>(windowAggregation, windowType);
}
WindowDefinitionPtr createWindowDefinition(const AttributeFieldPtr onKey, const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType) {
    return std::make_shared<WindowDefinition>(onKey, windowAggregation, windowType);
}

}// namespace NES