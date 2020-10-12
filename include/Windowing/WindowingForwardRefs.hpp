#ifndef NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_

#include "memory"

namespace NES {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

class WindowAggregation;
typedef std::shared_ptr<WindowAggregation> WindowAggregationPtr;

class WindowManager;
typedef std::shared_ptr<WindowManager> WindowManagerPtr;

template<class PartialAggregateType>
class WindowSliceStore;

class SliceMetaData;

class WindowType;
typedef std::shared_ptr<WindowType> WindowTypePtr;

class TumblingWindow;
typedef std::shared_ptr<TumblingWindow> TumblingWindowPtr;

class SlidingWindow;
typedef std::shared_ptr<SlidingWindow> SlidingWindowPtr;

class TimeMeasure;

class TimeCharacteristic;
typedef std::shared_ptr<TimeCharacteristic> TimeCharacteristicPtr;

class DistributionCharacteristic;
typedef std::shared_ptr<DistributionCharacteristic> DistributionCharacteristicPtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class ExpressionItem;

inline uint64_t getTsFromClock() {
    return time(NULL) * 1000;
}

}// namespace NES

#endif//NES_INCLUDE_WINDOWING_WINDOWINGFORWARDREFS_HPP_
