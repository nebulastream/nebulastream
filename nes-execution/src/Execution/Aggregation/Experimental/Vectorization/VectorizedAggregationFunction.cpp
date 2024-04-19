//
// Created by thai on 4/13/24.
//

#include <Execution/Aggregation/Experimental/Vectorization/VectorizedAggregationFunction.hpp>
#include <cstdint>

namespace NES::Runtime::Execution::Aggregation {

void* VectorizedAggregationFunction::getSliceStore() {
    return nullptr;
}

uint64_t VectorizedAggregationFunction::sum(void* /*buffer*/, uint64_t /*tid*/, uint64_t /*offset*/) {
    return 0;
}
}// namespace NES::Runtime::Execution::Aggregation