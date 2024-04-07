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
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/HyperLogLogDescriptor.hpp>
#include <cmath>
namespace NES::Statistic {

WindowStatisticDescriptorPtr HyperLogLogDescriptor::create(FieldAccessExpressionNodePtr field) {
    return create(std::move(field), DEFAULT_RELATIVE_ERROR);
}

WindowStatisticDescriptorPtr HyperLogLogDescriptor::create(FieldAccessExpressionNodePtr field, double error) {
    // Calculating 1.04/(error * error) and then rounding up to the next highest power of 2
    const auto numberOfBits = 1.04/(error * error);
    const auto nextHigherPowerOf2 = (uint64_t)std::pow(2, std::ceil(log(numberOfBits)/log(2)));
    return create(std::move(field), nextHigherPowerOf2);
}

WindowStatisticDescriptorPtr HyperLogLogDescriptor::create(FieldAccessExpressionNodePtr field, uint64_t width) {
    return std::make_shared<HyperLogLogDescriptor>(HyperLogLogDescriptor(field, width));
}

HyperLogLogDescriptor::HyperLogLogDescriptor(const FieldAccessExpressionNodePtr& field, const uint64_t width)
    : WindowStatisticDescriptor(field, width) {}

void HyperLogLogDescriptor::addDescriptorFields(Schema& outputSchema, const std::string& qualifierNameWithSeparator) {
    outputSchema.addField(qualifierNameWithSeparator + WIDTH_FIELD_NAME, BasicType::UINT64);
    outputSchema.addField(qualifierNameWithSeparator + ESTIMATE_FIELD_NAME, BasicType::FLOAT64);
    outputSchema.addField(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME, BasicType::TEXT);
}

HyperLogLogDescriptor::~HyperLogLogDescriptor() = default;

std::string HyperLogLogDescriptor::toString() { return "HyperLogLogDescriptor"; }

bool HyperLogLogDescriptor::equal(const WindowStatisticDescriptorPtr& rhs) const {
    if (rhs->instanceOf<HyperLogLogDescriptor>()) {
        auto rhsHyperLogLogDescriptor = rhs->as<HyperLogLogDescriptor>();
        return field->equal(rhsHyperLogLogDescriptor->field)
            && *triggerCondition == *rhsHyperLogLogDescriptor->triggerCondition
            && *sendingPolicy == *rhsHyperLogLogDescriptor->sendingPolicy
            && width == rhsHyperLogLogDescriptor->width;
    }
    return false;
}

}// namespace NES::Statistic