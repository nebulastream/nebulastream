#include <API/AbstractWindowDefinition.hpp>
#include <API/Window/WindowDefinition.hpp>
namespace NES {

WindowDefinition::WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, DistributionCharacteristicPtr distChar)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(nullptr), distributionType(distChar) {}

WindowDefinition::WindowDefinition(const AttributeFieldPtr onKey,
                                   const WindowAggregationPtr windowAggregation,
                                   const WindowTypePtr windowType,
                                   DistributionCharacteristicPtr distChar)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(onKey), distributionType(distChar) {
}

bool WindowDefinition::isKeyed() {
    return onKey != nullptr;
}

WindowDefinitionPtr createWindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, DistributionCharacteristicPtr distChar) {
    return std::make_shared<WindowDefinition>(windowAggregation, windowType, distChar);
}
WindowDefinitionPtr createWindowDefinition(const AttributeFieldPtr onKey, const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, DistributionCharacteristicPtr distChar) {
    return std::make_shared<WindowDefinition>(onKey, windowAggregation, windowType, distChar);
}

void WindowDefinition::setDistributionCharacteristic(DistributionCharacteristicPtr characteristic) {
    this->distributionType = characteristic;
}

DistributionCharacteristicPtr WindowDefinition::getDistributionType() {
    return distributionType;
}

}// namespace NES