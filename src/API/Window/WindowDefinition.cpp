#include <API/AbstractWindowDefinition.hpp>
#include <API/Window/WindowDefinition.hpp>
namespace NES {

WindowDefinition::WindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, DistributionCharacteristicPtr distChar)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(nullptr), distributionType(distChar), numberOfInputEdges(1) {}

WindowDefinition::WindowDefinition(const AttributeFieldPtr onKey,
                                   const WindowAggregationPtr windowAggregation,
                                   const WindowTypePtr windowType,
                                   DistributionCharacteristicPtr distChar,
                                   uint64_t numberOfInputEdges)
    : windowAggregation(windowAggregation), windowType(windowType), onKey(onKey), distributionType(distChar), numberOfInputEdges(numberOfInputEdges) {
}

bool WindowDefinition::isKeyed() {
    return onKey != nullptr;
}

WindowDefinitionPtr createWindowDefinition(const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, DistributionCharacteristicPtr distChar) {
    return std::make_shared<WindowDefinition>(windowAggregation, windowType, distChar);
}

WindowDefinitionPtr createWindowDefinition(const AttributeFieldPtr onKey, const WindowAggregationPtr windowAggregation, const WindowTypePtr windowType, DistributionCharacteristicPtr distChar, uint64_t numberOfInputEdges) {

    return std::make_shared<WindowDefinition>(onKey, windowAggregation, windowType, distChar, numberOfInputEdges);
}

void WindowDefinition::setDistributionCharacteristic(DistributionCharacteristicPtr characteristic) {
    this->distributionType = characteristic;
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
 WindowAggregationPtr& WindowDefinition::getWindowAggregation() {
    return windowAggregation;
}
WindowTypePtr& WindowDefinition::getWindowType(){
    return windowType;
}
AttributeFieldPtr& WindowDefinition::getOnKey(){
    return onKey;
}
void WindowDefinition::setWindowAggregation(WindowAggregationPtr& windowAggregation) {
    WindowDefinition::windowAggregation = windowAggregation;
}
void WindowDefinition::setWindowType(WindowTypePtr& windowType) {
    WindowDefinition::windowType = windowType;
}
void WindowDefinition::setOnKey(AttributeFieldPtr& onKey) {
    WindowDefinition::onKey = onKey;
}

}// namespace NES