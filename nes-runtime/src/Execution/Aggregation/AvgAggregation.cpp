#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
namespace NES::Runtime::Execution::Aggregation {

AvgAggregationFunction::AvgAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void AvgAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load memref
    auto oldValue = memref.load<Nautilus::Int64>();// TODO 3250 check the type
    // add the value
    auto newValue = oldValue + value;
    // put back to the memref
    memref.store(newValue);
}

void AvgAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memrefCount, Nautilus::Value<Nautilus::MemRef> memrefSum,Nautilus::Value<> value) {
    // load memref
    auto oldValueCount = memrefCount.load<Nautilus::Int64>();// TODO 3250 check the type
    auto oldValueSum = memrefSum.load<Nautilus::Int64>();// TODO 3250 check the type
    // add the value
    auto newValueCount = oldValueCount + 1;
    auto newValueSum = oldValueCount + value;
    // put back to the memref
    memrefCount.store(newValueCount);
    memrefSum.store(newValueSum);
}

void AvgAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memre2) {
    auto left = memref1.load<Nautilus::Int64>();
    auto right = memre2.load<Nautilus::Int64>();

    auto tmp = left + right;
    memref1.store(tmp);
}

Nautilus::Value<> AvgAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = memref.load<Nautilus::Int64>();// TODO 3250 check the type

    return finalVal;
}

void AvgAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::Int64>((int64_t) 0);
    memref.store(zero);
}

}// namespace NES::Runtime::Execution::Aggregation
