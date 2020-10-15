#ifndef NES_INCLUDE_API_WINDOWING_HPP_
#define NES_INCLUDE_API_WINDOWING_HPP_

#include <Windowing/WindowingForwardRefs.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>

namespace NES::API{

WindowAggregationPtr Sum(ExpressionItem);
WindowAggregationPtr Min(ExpressionItem);
WindowAggregationPtr Max(ExpressionItem);
WindowAggregationPtr Count();

TimeCharacteristicPtr EventTime(ExpressionItem);
TimeCharacteristicPtr ProcessingTime();

TimeMeasure Milliseconds(uint64_t);
TimeMeasure Seconds(uint64_t);
TimeMeasure Minutes(uint64_t);
TimeMeasure Hours(uint64_t);
TimeMeasure Days(uint64_t);

}

#endif//NES_INCLUDE_API_WINDOWING_HPP_
