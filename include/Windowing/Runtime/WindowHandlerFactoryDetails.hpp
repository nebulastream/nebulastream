#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORYDETAILS_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORYDETAILS_HPP_
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


class WindowHandlerFactoryDetails {
  public:

    /**
    * @brief Factory to create a typed window handler.
    * @tparam KeyType type of the key field
    * @tparam InputType type of the input aggregation field
    * @tparam PartialAggregateType type of the partial aggregation field
    * @tparam FinalAggregateType type of the final aggregation field
    * @param windowDefinition window definition
    * @param windowAggregation executable window aggregation
    * @return WindowHandlerPtr
    */
    template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
    static WindowHandlerPtr create(LogicalWindowDefinitionPtr windowDefinition,
                                   std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation) {
        return std::make_shared<WindowHandlerImpl<KeyType, InputType,PartialAggregateType , FinalAggregateType>>(windowDefinition, windowAggregation);
    }

    template<class KeyType, class InputType>
    static WindowHandlerPtr createWindowHandler(LogicalWindowDefinitionPtr windowDefinition){
        auto onField = windowDefinition->getWindowAggregation()->on();
        auto asField = windowDefinition->getWindowAggregation()->as();
        switch (windowDefinition->getWindowAggregation()->getType()) {
            case WindowAggregationDescriptor::AVG:
                return create<KeyType, InputType, AVGPartialType<InputType>, AVGResultType>(windowDefinition, ExecutableAVGAggregation<InputType>::create());
            case WindowAggregationDescriptor::Count:
                return create<KeyType, InputType, CountType, CountType>(windowDefinition, ExecutableCountAggregation<InputType>::create());
            case WindowAggregationDescriptor::Max:
                return create<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableMaxAggregation<InputType>::create());
            case WindowAggregationDescriptor::Min:
                return create<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableMinAggregation<InputType>::create());
            case WindowAggregationDescriptor::Sum:
                return create<KeyType, InputType, InputType, InputType>(windowDefinition, ExecutableSumAggregation<InputType>::create());;
        }
        NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: AVG aggregation currently not supported");
    };

    template<typename KeyType>
    static WindowHandlerPtr createWindowHandler(LogicalWindowDefinitionPtr windowDefinition) {
        auto logicalAggregationInput = windowDefinition->getWindowAggregation()->on();
        auto physicalInputType = DefaultPhysicalTypeFactory().getPhysicalType(logicalAggregationInput->getStamp());
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
                case BasicPhysicalType::CHAR:
                case BasicPhysicalType::BOOLEAN: NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: we dont support aggregation of Chars or Booleans");
            };
        }
        NES_THROW_RUNTIME_ERROR("WindowHandlerFactory: currently we dont support non basic input types");
    };

};
}// namespace NES

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORYDETAILS_HPP_
