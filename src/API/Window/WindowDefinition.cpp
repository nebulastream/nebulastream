#include <API/AbstractWindowDefinition.hpp>
#include <API/Window/WindowDefinition.hpp>
namespace NES {

WindowDefinition::WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, const DistributionCharacteristicPtr distrType)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(nullptr), distributionType(distrType) {}

WindowDefinition::WindowDefinition(const AttributeFieldPtr onKey,
                                   const WindowAggregationPtr windowAggregation,
                                   const WindowTypePtr windowType,
                                   const DistributionCharacteristicPtr distrType
                                   )
    : windowAggregation(windowAggregation), windowType(windowType), onKey(onKey), distributionType(distrType) {
}

bool WindowDefinition::isKeyed() {
    return onKey != nullptr;
}

WindowDefinitionPtr createWindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, const DistributionCharacteristicPtr distrType) {
    return std::make_shared<WindowDefinition>(windowAggregation, windowType, distrType);
}
WindowDefinitionPtr createWindowDefinition(const AttributeFieldPtr onKey, const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, const DistributionCharacteristicPtr distrType) {
    return std::make_shared<WindowDefinition>(onKey, windowAggregation, windowType, distrType);
}


DistributionCharacteristicPtr WindowDefinition::getDistributionType()
{
    return distributionType;
}


}// namespace NES