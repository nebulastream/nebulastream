
#include <Windowing/Runtime/WindowHandlerFactory.hpp>
#include <Windowing/Runtime/WindowHandlerFactoryDetails.hpp>

namespace NES::Windowing {


WindowHandlerPtr WindowHandlerFactory::createWindowHandler(LogicalWindowDefinitionPtr windowDefinition) {

        if (windowDefinition->isKeyed()) {
            auto logicalKeyType = windowDefinition->getOnKey()->getStamp();
            auto physicalKeyType = DefaultPhysicalTypeFactory().getPhysicalType(logicalKeyType);
            if (physicalKeyType->isBasicType()) {
                auto basicKeyType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalKeyType);
                switch (basicKeyType->getNativeType()) {
                    case BasicPhysicalType::UINT_8: return WindowHandlerFactoryDetails::createWindowHandler<uint8_t>(windowDefinition);
                    case BasicPhysicalType::UINT_16: return WindowHandlerFactoryDetails::createWindowHandler<uint16_t>(windowDefinition);
                    case BasicPhysicalType::UINT_32: return WindowHandlerFactoryDetails::createWindowHandler<uint32_t>(windowDefinition);
                    case BasicPhysicalType::UINT_64: return WindowHandlerFactoryDetails::createWindowHandler<uint64_t>(windowDefinition);
                    case BasicPhysicalType::INT_8: return WindowHandlerFactoryDetails::createWindowHandler<int8_t>(windowDefinition);
                    case BasicPhysicalType::INT_16: return WindowHandlerFactoryDetails::createWindowHandler<int16_t>(windowDefinition);
                    case BasicPhysicalType::INT_32: return WindowHandlerFactoryDetails::createWindowHandler<int32_t>(windowDefinition);
                    case BasicPhysicalType::INT_64: return WindowHandlerFactoryDetails::createWindowHandler<int64_t>(windowDefinition);
                    case BasicPhysicalType::FLOAT: return WindowHandlerFactoryDetails::createWindowHandler<float>(windowDefinition);
                    case BasicPhysicalType::DOUBLE: return WindowHandlerFactoryDetails::createWindowHandler<double>(windowDefinition);
                    case BasicPhysicalType::CHAR: return WindowHandlerFactoryDetails::createWindowHandler<char>(windowDefinition);
                    case BasicPhysicalType::BOOLEAN: return WindowHandlerFactoryDetails::createWindowHandler<bool>(windowDefinition);
                };
            } else {
                NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non basic key types");
            }
        }
        NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non keyed aggregations");
}

}