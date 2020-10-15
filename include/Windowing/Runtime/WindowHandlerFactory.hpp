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


class WindowHandlerFactory {
  public:
    /**
     * @brief Creates a window handler only by using the window definition.
     * @param windowDefinition window definition
     * @return WindowHandlerPtr
     */
    static WindowHandlerPtr createWindowHandler(LogicalWindowDefinitionPtr windowDefinition);
};
}// namespace NES

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
