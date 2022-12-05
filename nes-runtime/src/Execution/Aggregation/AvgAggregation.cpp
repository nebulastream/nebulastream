#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
namespace NES::Runtime::Execution::Aggregation {

AvgAggregationFunction::AvgAggregationFunction(const DataTypePtr& inputType, const DataTypePtr& finalType)
    : AggregationFunction(inputType, finalType) {}

void AvgAggregationFunction::lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Value<> value) {
    // load memref
    auto oldSum = memref.load<Nautilus::Int64>();// TODO 3280 check the type

    auto countMemref = (memref+offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto oldCount = countMemref.load<Nautilus::Int64>();

    // add the value
    auto newSum = oldSum + value;
    auto newCount = oldCount +1;
    // put back to the memref
    memref.store(newSum);
    countMemref.store(newCount);
}

void AvgAggregationFunction::combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) {
    auto sumLeft = memref1.load<Nautilus::Int64>();
    auto countLeftMemref = (memref1+offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto countLeft = countLeftMemref.load<Nautilus::Int64>();

    auto sumRight = memref2.load<Nautilus::Int64>();
    auto countrightMemref = (memref2+offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto countright = countrightMemref.load<Nautilus::Int64>();


    auto tmpSum = sumLeft + sumRight;
    auto tmpCount = countLeft + countright;

    memref1.store(tmpSum);
    countLeftMemref.store(tmpCount);
}

Nautilus::Value<> AvgAggregationFunction::lower(Nautilus::Value<Nautilus::MemRef> memref) {
    auto sum = memref.load<Nautilus::Int64>();// TODO 3280 check the type

    auto countMemref = (memref+offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();
    auto count = countMemref.load<Nautilus::Int64>();

    auto finalVal = sum / count;

    return finalVal;
}

void AvgAggregationFunction::reset(Nautilus::Value<Nautilus::MemRef> memref) {
    auto zero = Nautilus::Value<Nautilus::Int64>((int64_t) 0);
    auto countMemref = (memref+offsetof(AvgAggregationValue, count)).as<Nautilus::MemRef>();

    memref.store(zero);
    countMemref.store(zero);
}

}// namespace NES::Runtime::Execution::Aggregation
