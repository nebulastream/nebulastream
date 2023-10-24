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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/Exceptions/InvalidFieldException.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>

namespace NES::Windowing {

TimeBasedWindowType::TimeBasedWindowType(TimeCharacteristicPtr timeCharacteristic)
    : timeCharacteristic(std::move(timeCharacteristic)) {}

bool TimeBasedWindowType::isTimeBasedWindowType() { return true; }

bool TimeBasedWindowType::inferStamp(const SchemaPtr& schema) {
    if (timeCharacteristic->getType() == TimeCharacteristic::Type::EventTime) {
        auto fieldName = timeCharacteristic->getField()->getName();
        auto existingField = schema->hasFieldName(fieldName);
        if (existingField) {
            timeCharacteristic->getField()->setName(existingField->getName());
            return true;
        } else if (fieldName == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME) {
            return true;
        } else {
            NES_ERROR("TimeBasedWindow using a non existing time field  {}", fieldName);
            throw InvalidFieldException("TimeBasedWindow using a non existing time field " + fieldName);
        }
    }
    return true;
}

TimeCharacteristicPtr TimeBasedWindowType::getTimeCharacteristic() const { return timeCharacteristic; }

}// namespace NES::Windowing
