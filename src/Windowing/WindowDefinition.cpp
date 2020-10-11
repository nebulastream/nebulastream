#include <Windowing/WindowDefinition.hpp>
#include <utility>
namespace NES {

WindowDefinition::WindowDefinition(WindowAggregationPtr windowAggregation, WindowTypePtr windowType, DistributionCharacteristicPtr distChar)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(nullptr), distributionType(std::move(distChar)), numberOfInputEdges(1) {}

WindowDefinition::WindowDefinition(AttributeFieldPtr onKey,
                                   WindowAggregationPtr windowAggregation,
                                   WindowTypePtr windowType,
                                   DistributionCharacteristicPtr distChar,
                                   uint64_t numberOfInputEdges)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(std::move(onKey)), distributionType(std::move(distChar)), numberOfInputEdges(numberOfInputEdges) {
}

bool WindowDefinition::isKeyed() {
    return onKey != nullptr;
}

WindowDefinitionPtr WindowDefinition::create(WindowAggregationPtr windowAggregation, WindowTypePtr windowType, DistributionCharacteristicPtr distChar) {
    return std::make_shared<WindowDefinition>(windowAggregation, windowType, distChar);
}

WindowDefinitionPtr WindowDefinition::create(AttributeFieldPtr onKey, WindowAggregationPtr windowAggregation, WindowTypePtr windowType, DistributionCharacteristicPtr distChar, uint64_t numberOfInputEdges) {

    return std::make_shared<WindowDefinition>(onKey, windowAggregation, windowType, distChar, numberOfInputEdges);
}

void WindowDefinition::setDistributionCharacteristic(DistributionCharacteristicPtr characteristic) {
    this->distributionType = std::move(characteristic);
}

DistributionCharacteristicPtr WindowDefinition::getDistributionType() {
    return distributionType;
}
uint64_t WindowDefinition::getNumberOfInputEdges() const {
    return numberOfInputEdges;
}
void WindowDefinition::setNumberOfInputEdges(uint64_t numberOfInputEdges) {
    this->numberOfInputEdges = numberOfInputEdges;
}
WindowAggregationPtr WindowDefinition::getWindowAggregation() {
    return windowAggregation;
}
WindowTypePtr WindowDefinition::getWindowType() {
    return windowType;
}
AttributeFieldPtr WindowDefinition::getOnKey() {
    return onKey;
}
void WindowDefinition::setWindowAggregation(WindowAggregationPtr windowAggregation) {
    this->windowAggregation = std::move(windowAggregation);
}
void WindowDefinition::setWindowType(WindowTypePtr windowType) {
    this->windowType = std::move(windowType);
}
void WindowDefinition::setOnKey(AttributeFieldPtr onKey) {
    this->onKey = std::move(onKey);
}

}// namespace NES