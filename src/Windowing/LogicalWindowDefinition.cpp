#include <Util/Logger.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <utility>
namespace NES {

LogicalWindowDefinition::LogicalWindowDefinition(WindowAggregationPtr windowAggregation, WindowTypePtr windowType, DistributionCharacteristicPtr distChar)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(nullptr), distributionType(std::move(distChar)), numberOfInputEdges(1) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}

LogicalWindowDefinition::LogicalWindowDefinition(AttributeFieldPtr onKey,
                                   WindowAggregationPtr windowAggregation,
                                   WindowTypePtr windowType,
                                   DistributionCharacteristicPtr distChar,
                                   uint64_t numberOfInputEdges)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(std::move(onKey)), distributionType(std::move(distChar)), numberOfInputEdges(numberOfInputEdges) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}

bool LogicalWindowDefinition::isKeyed() {
    return onKey != nullptr;
}

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(WindowAggregationPtr windowAggregation, WindowTypePtr windowType, DistributionCharacteristicPtr distChar) {
    return std::make_shared<LogicalWindowDefinition>(windowAggregation, windowType, distChar);
}

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(AttributeFieldPtr onKey, WindowAggregationPtr windowAggregation, WindowTypePtr windowType, DistributionCharacteristicPtr distChar, uint64_t numberOfInputEdges) {
    return std::make_shared<LogicalWindowDefinition>(onKey, windowAggregation, windowType, distChar, numberOfInputEdges);
}

void LogicalWindowDefinition::setDistributionCharacteristic(DistributionCharacteristicPtr characteristic) {
    this->distributionType = std::move(characteristic);
}

DistributionCharacteristicPtr LogicalWindowDefinition::getDistributionType() {
    return distributionType;
}
uint64_t LogicalWindowDefinition::getNumberOfInputEdges() const {
    return numberOfInputEdges;
}
void LogicalWindowDefinition::setNumberOfInputEdges(uint64_t numberOfInputEdges) {
    this->numberOfInputEdges = numberOfInputEdges;
}
WindowAggregationPtr LogicalWindowDefinition::getWindowAggregation() {
    return windowAggregation;
}
WindowTypePtr LogicalWindowDefinition::getWindowType() {
    return windowType;
}
AttributeFieldPtr LogicalWindowDefinition::getOnKey() {
    return onKey;
}
void LogicalWindowDefinition::setWindowAggregation(WindowAggregationPtr windowAggregation) {
    this->windowAggregation = std::move(windowAggregation);
}
void LogicalWindowDefinition::setWindowType(WindowTypePtr windowType) {
    this->windowType = std::move(windowType);
}
void LogicalWindowDefinition::setOnKey(AttributeFieldPtr onKey) {
    this->onKey = std::move(onKey);
}

}// namespace NES