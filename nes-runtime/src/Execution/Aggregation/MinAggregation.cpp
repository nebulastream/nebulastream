#include <Execution/Aggregation/MinAggregation.hpp>

namespace NES::Runtime::Execution::Aggregation {

MinAggregationFunction::MinAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void MinAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load
    auto oldValue = memref.load<Nautilus::Int64>();
    // compare
    // TODO: make std::min(oldValue, value) possible?
    // otherwise bithack for min/max
    auto newValue = (value < oldValue) ? value : oldValue;
    // store
    memref.store(newValue);
}

void MinAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto left = memref1.load<Nautilus::Int64>();
    auto right = memref2.load<Nautilus::Int64>();
    auto tmp = std::min(left, right);
    memref1.store(tmp);
}

Nautilus::Value<> MinAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = memref.load<Nautilus::Int64>();// TODO 3250 check the type
    return memref.load<Nautilus::Int64>();
}

void MinAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::Int64>((int64_t) 0);
    memref.store(zero);
}

}