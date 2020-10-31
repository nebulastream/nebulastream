#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <Windowing/WindowAggregations/ExecutableAVGAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableCountAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMinAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>
#include <memory>
namespace NES::Windowing {

class WindowHandlerFactory {
  public:
    /**
     * @brief Creates a window handler for aggregations only by using the window definition.
     * @param windowDefinition window definition
     * @return WindowHandlerPtr
     */
    static WindowHandlerPtr createAggregationWindowHandler(LogicalWindowDefinitionPtr windowDefinition);

};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
