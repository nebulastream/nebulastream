/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <StatisticIdentifiers.hpp>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/CountMinDescriptor.hpp>
#include <cmath>
#include <utility>
namespace NES::Statistic {

CountMinDescriptor::CountMinDescriptor(const FieldAccessExpressionNodePtr& field, const uint64_t width, uint64_t depth)
    : WindowStatisticDescriptor(field, width), depth(depth) {}

WindowStatisticDescriptorPtr CountMinDescriptor::create(FieldAccessExpressionNodePtr field) {
    return create(std::move(field), DEFAULT_RELATIVE_ERROR, DEFAULT_ERROR_PROBABILITY);
}

WindowStatisticDescriptorPtr CountMinDescriptor::create(FieldAccessExpressionNodePtr field, double error, double probability) {
    const auto calcWidth = (uint64_t)std::ceil(std::exp(1) / (probability));
    const auto calcDepth = (uint64_t)std::ceil(std::log(1.0 / error));
    return create(std::move(field), calcWidth, calcDepth);
}

WindowStatisticDescriptorPtr CountMinDescriptor::create(FieldAccessExpressionNodePtr field, uint64_t width, uint64_t depth) {
    return std::make_shared<CountMinDescriptor>(CountMinDescriptor(field, width, depth));
}

uint64_t CountMinDescriptor::getDepth() const { return depth; }

void CountMinDescriptor::addDescriptorFields(Schema& outputSchema, const std::string& qualifierNameWithSeparator) {
    outputSchema.addField(qualifierNameWithSeparator + WIDTH_FIELD_NAME, BasicType::UINT64);
    outputSchema.addField(qualifierNameWithSeparator + DEPTH_FIELD_NAME, BasicType::UINT64);
    outputSchema.addField(qualifierNameWithSeparator + NUMBER_OF_BITS_IN_KEY, BasicType::UINT64);
    outputSchema.addField(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME, BasicType::TEXT);
}
CountMinDescriptor::~CountMinDescriptor() = default;

std::string CountMinDescriptor::toString() { return "CountMinDescriptor"; }

bool CountMinDescriptor::equal(const WindowStatisticDescriptorPtr& rhs) const {
    if (rhs->instanceOf<CountMinDescriptor>()) {
        auto rhsCountMinDescriptor = rhs->as<CountMinDescriptor>();
        return field->equal(rhsCountMinDescriptor->field)
            && *triggerCondition == *rhsCountMinDescriptor->triggerCondition
            && *sendingPolicy == *rhsCountMinDescriptor->sendingPolicy
            && depth == rhsCountMinDescriptor->depth
            && width == rhsCountMinDescriptor->width;
    }
    return false;
}

}// namespace NES::Statistic