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
#include <API/AttributeField.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/ReservoirSampleDescriptor.hpp>
#include <StatisticIdentifiers.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

WindowStatisticDescriptorPtr ReservoirSampleDescriptor::create(FieldAccessExpressionNodePtr field) {
    return create(field, false);
}

WindowStatisticDescriptorPtr ReservoirSampleDescriptor::create(FieldAccessExpressionNodePtr field, bool keepOnlyRequiredField) {
    return create(field, DEFAULT_SAMPLE_SIZE, keepOnlyRequiredField);
}

WindowStatisticDescriptorPtr
ReservoirSampleDescriptor::create(FieldAccessExpressionNodePtr field, uint64_t width, bool keepOnlyRequiredField) {
    return std::make_shared<ReservoirSampleDescriptor>(ReservoirSampleDescriptor(field, width, keepOnlyRequiredField));
}

bool ReservoirSampleDescriptor::equal(const WindowStatisticDescriptorPtr& rhs) const {
    if (rhs->instanceOf<ReservoirSampleDescriptor>()) {
        auto rhsReservoirSampleDescriptor = rhs->as<ReservoirSampleDescriptor>();
        return field->equal(rhsReservoirSampleDescriptor->field)
            && width == rhsReservoirSampleDescriptor->width
            && keepOnlyRequiredField == rhsReservoirSampleDescriptor->keepOnlyRequiredField;
    }
    return false;
}

void ReservoirSampleDescriptor::addDescriptorFields(Schema& inputSchema, Schema& outputSchema, const std::string& qualifierNameWithSeparator) {
    outputSchema.addField(qualifierNameWithSeparator + WIDTH_FIELD_NAME, BasicType::UINT64);
    outputSchema.addField(qualifierNameWithSeparator + STATISTIC_DATA_FIELD_NAME, BasicType::TEXT);

    if (keepOnlyRequiredField) {
        const auto& requiredField = inputSchema.getField(field->getFieldName());
        NES_INFO("Copying only field {} to the output schema", requiredField->toString());
        outputSchema.addField(requiredField->copy());
    } else {
        NES_INFO("Copying all fields of the input schema to the output schema");
        outputSchema.copyFields(inputSchema);
    }
}

std::string ReservoirSampleDescriptor::toString() { return "ReservoirSampleDescriptor"; }

bool ReservoirSampleDescriptor::isKeepOnlyRequiredField() const { return keepOnlyRequiredField; }

ReservoirSampleDescriptor::~ReservoirSampleDescriptor() = default;

ReservoirSampleDescriptor::ReservoirSampleDescriptor(const FieldAccessExpressionNodePtr& field, uint64_t width, bool keepOnlyRequiredField)
    : WindowStatisticDescriptor(field, width), keepOnlyRequiredField(keepOnlyRequiredField) {}

}// namespace NES::Statistic