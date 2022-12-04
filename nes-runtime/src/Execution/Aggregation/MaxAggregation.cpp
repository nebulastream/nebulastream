#include <Execution/Aggregation/MaxAggregation.hpp>

namespace NES::Runtime::Execution::Aggregation {
MaxAggregationFunction::MaxAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void MaxAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load
    auto oldValue = memref.load<Nautilus::Int64>();
    // compare
    // TODO: make std::max(oldValue, value) possible?
    // otherwise bithack for min/max
    auto newValue = (value > oldValue) ? value : oldValue;
    // store
    memref.store(newValue);
}

void MaxAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto left = memref1.load<Nautilus::Int64>();
    auto right = memref2.load<Nautilus::Int64>();
    auto tmp = std::max(left, right);
    memref1.store(tmp);
}

Nautilus::Value<> MaxAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto finalVal = memref.load<Nautilus::Int64>();// TODO 3250 check the type
    return memref.load<Nautilus::Int64>();
}

void MaxAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::Int64>((int64_t) 0);
    memref.store(zero);
}
}