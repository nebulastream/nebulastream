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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/EquiWidthHistogramDescriptor.hpp>

namespace NES::Statistic {

WindowStatisticDescriptorPtr EquiWidthHistogramDescriptor::create(FieldAccessExpressionNodePtr field) {
    return create(field, DEFAULT_BIN_WIDTH);
}

WindowStatisticDescriptorPtr EquiWidthHistogramDescriptor::create(FieldAccessExpressionNodePtr field, uint64_t binWidth) {
    return std::make_shared<EquiWidthHistogramDescriptor>(EquiWidthHistogramDescriptor(field, binWidth));
}

bool EquiWidthHistogramDescriptor::equal(const WindowStatisticDescriptorPtr& rhs) const {
    if (rhs->instanceOf<EquiWidthHistogramDescriptor>()) {
        auto rhsEquiWidthHistDescriptor = rhs->as<EquiWidthHistogramDescriptor>();
        return field->equal(rhsEquiWidthHistDescriptor->field) && width == rhsEquiWidthHistDescriptor->width;
    }
    return false;
}

std::string EquiWidthHistogramDescriptor::toString() { return "EquiWidthHistogramDescriptor"; }

StatisticSynopsisType NES::Statistic::EquiWidthHistogramDescriptor::getType() const { return StatisticSynopsisType::EQUI_WIDTH_HISTOGRAM; }

EquiWidthHistogramDescriptor::~EquiWidthHistogramDescriptor() = default;

void EquiWidthHistogramDescriptor::addDescriptorFields(Schema&,
                                                       Schema& outputSchema,
                                                       const std::string& qualifierNameWithSeparator) {
    using enum BasicType;
    outputSchema.addField(qualifierNameWithSeparator + WIDTH_FIELD_NAME, UINT64);
    outputSchema.addField(qualifierNameWithSeparator + NUMBER_OF_BINS, UINT64);
    outputSchema.addField(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME, TEXT);
}

EquiWidthHistogramDescriptor::EquiWidthHistogramDescriptor(FieldAccessExpressionNodePtr field, uint64_t binWidth)
    : WindowStatisticDescriptor(field, binWidth) {}


} // namespace NES::Statistic