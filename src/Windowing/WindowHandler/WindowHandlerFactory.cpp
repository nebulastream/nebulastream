#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactory.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactoryDetails.hpp>

namespace NES::Windowing {

AbstractWindowHandlerPtr WindowHandlerFactory::createAggregationWindowHandler(LogicalWindowDefinitionPtr windowDefinition) {
    if (windowDefinition->isKeyed()) {
        auto logicalKeyType = windowDefinition->getOnKey()->getStamp();
        auto physicalKeyType = DefaultPhysicalTypeFactory().getPhysicalType(logicalKeyType);
        if (physicalKeyType->isBasicType()) {
            auto basicKeyType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalKeyType);
            switch (basicKeyType->getNativeType()) {
                case BasicPhysicalType::UINT_8: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<uint8_t>(windowDefinition);
                case BasicPhysicalType::UINT_16: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<uint16_t>(windowDefinition);
                case BasicPhysicalType::UINT_32: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<uint32_t>(windowDefinition);
                case BasicPhysicalType::UINT_64: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<uint64_t>(windowDefinition);
                case BasicPhysicalType::INT_8: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<int8_t>(windowDefinition);
                case BasicPhysicalType::INT_16: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<int16_t>(windowDefinition);
                case BasicPhysicalType::INT_32: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<int32_t>(windowDefinition);
                case BasicPhysicalType::INT_64: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<int64_t>(windowDefinition);
                case BasicPhysicalType::FLOAT: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<float>(windowDefinition);
                case BasicPhysicalType::DOUBLE: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<double>(windowDefinition);
                case BasicPhysicalType::CHAR: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<char>(windowDefinition);
                case BasicPhysicalType::BOOLEAN: return WindowHandlerFactoryDetails::createWindowHandlerForAggregationKeyType<bool>(windowDefinition);
            }
        } else {
            NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non basic key types");
        }
    }
    NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non keyed aggregations");
}

}// namespace NES::Windowing