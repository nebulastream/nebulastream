#ifndef NES_INCLUDE_API_WINDOWING_HPP_
#define NES_INCLUDE_API_WINDOWING_HPP_

#include <Windowing/WindowingForwardRefs.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>

/**
 * @brief The following declares API functions for windowing.
 */
namespace NES::API{

/**
 * @brief Defines a Sum Aggregation function on a particular field.
 * @param ExpressionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
WindowAggregationPtr Sum(ExpressionItem);

/**
 * @brief Defines a Min Aggregation function on a particular field.
 * @param ExpressionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
WindowAggregationPtr Min(ExpressionItem);

/**
 * @brief Defines a Max Aggregation function on a particular field.
 * @param ExpressionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
WindowAggregationPtr Max(ExpressionItem);

/**
 * @brief Defines a Cun Aggregation function on a particular field.
 * @return A descriptor of the aggregation function.
 */
WindowAggregationPtr Count();

/**
 * @brief Defines event time as a time characteristic for a window.
 * @param ExpressionItem which defines the field name.
 * @return A descriptor of the time characteristic.
 */
TimeCharacteristicPtr EventTime(ExpressionItem);

/**
 * @brief Defines a processing time as a time characteristic for a window.
 * @return A descriptor of the time characteristic.
 */
TimeCharacteristicPtr ProcessingTime();

/**
 * @brief A time measure in Milliseconds.
 * @return TimeMeasure
 */
TimeMeasure Milliseconds(uint64_t);

/**
 * @brief A time measure in Seconds.
 * @return TimeMeasure
 */
TimeMeasure Seconds(uint64_t);

/**
 * @brief A time measure in Minutes.
 * @return TimeMeasure
 */
TimeMeasure Minutes(uint64_t);

/**
 * @brief A time measure in Hours.
 * @return TimeMeasure
 */
TimeMeasure Hours(uint64_t);

/**
 * @brief A time measure in Days.
 * @return TimeMeasure
 */
TimeMeasure Days(uint64_t);

}

#endif//NES_INCLUDE_API_WINDOWING_HPP_
