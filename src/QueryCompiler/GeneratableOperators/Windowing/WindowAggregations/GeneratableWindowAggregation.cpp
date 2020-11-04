
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
#include <utility>

namespace NES {

GeneratableWindowAggregation::GeneratableWindowAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor) : aggregationDescriptor(std::move(aggregationDescriptor)) {}

}// namespace NES