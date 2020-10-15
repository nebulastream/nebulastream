
#include <API/Expressions/Expressions.hpp>
#include <API/Windowing.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/TimeCharacteristic.hpp>

namespace NES::API {

WindowAggregationPtr Sum(ExpressionItem onField) {
    return SumAggregationDescriptor::on(onField);
}

WindowAggregationPtr Min(ExpressionItem onField) {
    return MinAggregationDescriptor::on(onField);
}

WindowAggregationPtr Max(ExpressionItem onField) {
    return MaxAggregationDescriptor::on(onField);
}

WindowAggregationPtr Count() {
    return CountAggregationDescriptor::on();
}

TimeMeasure Milliseconds(uint64_t milliseconds) {
    return TimeMeasure(milliseconds); }

TimeMeasure Seconds(uint64_t seconds) {
    return Milliseconds(seconds * 1000); }

TimeMeasure Minutes(uint64_t minutes) {
    return Seconds(minutes * 60); }

TimeMeasure Hours(uint64_t hours) {
    return Minutes(hours * 60);
}

TimeMeasure Days(uint64_t days) {
    return Hours(days);
}

TimeCharacteristicPtr EventTime(ExpressionItem onField){
    return TimeCharacteristic::createEventTime(onField);
}

TimeCharacteristicPtr ProcessingTime(){
    return TimeCharacteristic::createProcessingTime();
}

}// namespace NES::API