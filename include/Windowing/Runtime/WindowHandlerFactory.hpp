#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <Windowing/Runtime/WindowHandlerImpl.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/ExecutableAVGAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableCountAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMinAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>

namespace NES {

template<typename SumType>
class AVGPartialType;

class WindowHandlerFactory {
  public:
    template<class KeyType, class InputType, class FinalAggregateType, class PartialAggregateType>
    static WindowHandlerPtr create(LogicalWindowDefinitionPtr windowDefinition,
                                   std::shared_ptr<ExecutableWindowAggregation<InputType, FinalAggregateType, PartialAggregateType>> windowAggregation) {
        return std::make_shared<WindowHandlerImpl<KeyType, InputType, FinalAggregateType, PartialAggregateType>>(windowDefinition, windowAggregation);
    }

    template<class KeyType, class InputType>
    static WindowHandlerPtr createWindowHandler(LogicalWindowDefinitionPtr windowDefinition){
        auto onField = windowDefinition->getWindowAggregation()->on();
        auto asField = windowDefinition->getWindowAggregation()->as();
        switch (windowDefinition->getWindowAggregation()->getType()) {
            case WindowAggregationDescriptor::AVG:
                return create<KeyType, InputType, AVGPartialType<InputType>, AVGResultType>(windowDefinition, ExecutableAVGAggregation<InputType>::create(onField, asField));
            case WindowAggregationDescriptor::Count:
                return create<KeyType, InputType, CountType, CountType>(windowDefinition, ExecutableCountAggregation<InputType>::create(onField, asField));
            case WindowAggregationDescriptor::Max:
                return create<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableMaxAggregation<InputType>::create(onField, asField));
            case WindowAggregationDescriptor::Min:
                return create<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableMinAggregation<InputType>::create(onField, asField));
            case WindowAggregationDescriptor::Sum:
                return create<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableSumAggregation<InputType>::create(onField, asField));;
        }
    };

    template<typename KeyType>
    static WindowHandlerPtr createWindowHandler(LogicalWindowDefinitionPtr windowDefinition) {
        auto logicalAggregationInput = windowDefinition->getWindowAggregation()->on();
        auto physicalInputType = DefaultPhysicalTypeFactory().getPhysicalType(logicalAggregationInput->getDataType());
        if (physicalInputType->isBasicType()) {
            auto basicInputType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalInputType);
            switch (basicInputType->getNativeType()) {
                case BasicPhysicalType::UINT_8: return createWindowHandler<KeyType, uint8_t>(windowDefinition);
                case BasicPhysicalType::UINT_16: return createWindowHandler<KeyType, uint16_t>(windowDefinition);
                case BasicPhysicalType::UINT_32: return createWindowHandler<KeyType, uint32_t>(windowDefinition);
                case BasicPhysicalType::UINT_64: return createWindowHandler<KeyType, uint64_t>(windowDefinition);
                case BasicPhysicalType::INT_8: return createWindowHandler<KeyType, int8_t>(windowDefinition);
                case BasicPhysicalType::INT_16: return createWindowHandler<KeyType, int16_t>(windowDefinition);
                case BasicPhysicalType::INT_32: return createWindowHandler<KeyType, int32_t>(windowDefinition);
                case BasicPhysicalType::INT_64: return createWindowHandler<KeyType, int64_t>(windowDefinition);
                case BasicPhysicalType::FLOAT: return createWindowHandler<KeyType, float>(windowDefinition);
                case BasicPhysicalType::DOUBLE: return createWindowHandler<KeyType, double>(windowDefinition);
                case BasicPhysicalType::CHAR: return createWindowHandler<KeyType, char>(windowDefinition);
                case BasicPhysicalType::BOOLEAN: return createWindowHandler<KeyType, bool>(windowDefinition);
            };
        } else {
            NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non basic input types");
        }
    };

    static WindowHandlerPtr createWindowHandler(LogicalWindowDefinitionPtr windowDefinition) {

        if (windowDefinition->isKeyed()) {
            auto logicalKeyType = windowDefinition->getOnKey()->getDataType();
            auto physicalKeyType = DefaultPhysicalTypeFactory().getPhysicalType(logicalKeyType);
            if (physicalKeyType->isBasicType()) {
                auto basicKeyType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalKeyType);
                switch (basicKeyType->getNativeType()) {
                    case BasicPhysicalType::UINT_8: return createWindowHandler<uint8_t>(windowDefinition);
                    case BasicPhysicalType::UINT_16: return createWindowHandler<uint16_t>(windowDefinition);
                    case BasicPhysicalType::UINT_32: return createWindowHandler<uint32_t>(windowDefinition);
                    case BasicPhysicalType::UINT_64: return createWindowHandler<uint64_t>(windowDefinition);
                    case BasicPhysicalType::INT_8: return createWindowHandler<int8_t>(windowDefinition);
                    case BasicPhysicalType::INT_16: return createWindowHandler<int16_t>(windowDefinition);
                    case BasicPhysicalType::INT_32: return createWindowHandler<int32_t>(windowDefinition);
                    case BasicPhysicalType::INT_64: return createWindowHandler<int64_t>(windowDefinition);
                    case BasicPhysicalType::FLOAT: return createWindowHandler<float>(windowDefinition);
                    case BasicPhysicalType::DOUBLE: return createWindowHandler<double>(windowDefinition);
                    case BasicPhysicalType::CHAR: return createWindowHandler<char>(windowDefinition);
                    case BasicPhysicalType::BOOLEAN: return createWindowHandler<bool>(windowDefinition);
                };
            } else {
                NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non basic key types");
            }
        } else {
            NES_NOT_IMPLEMENTED();
        }
    };
};
}// namespace NES

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
