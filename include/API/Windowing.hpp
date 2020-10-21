#ifndef NES_INCLUDE_API_WINDOWING_HPP_
#define NES_INCLUDE_API_WINDOWING_HPP_

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowingForwardRefs.hpp>


/**
 * @brief The following declares API functions for windowing.
 */
namespace NES::API{

/**
 * @brief Defines a Sum Aggregation function on a particular field.
 * @param ExpressionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
Windowing::WindowAggregationPtr Sum(ExpressionItem);

/**
 * @brief Defines a Min Aggregation function on a particular field.
 * @param ExpressionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
Windowing::WindowAggregationPtr Min(ExpressionItem);

/**
 * @brief Defines a Max Aggregation function on a particular field.
 * @param ExpressionItem Attribute("field-name") the field which should be aggregated.
 * @return A descriptor of the aggregation function.
 */
Windowing::WindowAggregationPtr Max(ExpressionItem);

/**
 * @brief Defines a Cun Aggregation function on a particular field.
 * @return A descriptor of the aggregation function.
 */
Windowing::WindowAggregationPtr Count();

/**
 * @brief Defines event time as a time characteristic for a window.
 * @param ExpressionItem which defines the field name.
 * @return A descriptor of the time characteristic.
 */
Windowing::TimeCharacteristicPtr EventTime(ExpressionItem);

/**
 * @brief Defines a processing time as a time characteristic for a window.
 * @return A descriptor of the time characteristic.
 */
Windowing::TimeCharacteristicPtr ProcessingTime();

/**
 * @brief A time measure in Milliseconds.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Milliseconds(uint64_t);

/**
 * @brief A time measure in Seconds.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Seconds(uint64_t);

/**
 * @brief A time measure in Minutes.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Minutes(uint64_t);

/**
 * @brief A time measure in Hours.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Hours(uint64_t);

/**
 * @brief A time measure in Days.
 * @return TimeMeasure
 */
Windowing::TimeMeasure Days(uint64_t);

}

#endif//NES_INCLUDE_API_WINDOWING_HPP_
