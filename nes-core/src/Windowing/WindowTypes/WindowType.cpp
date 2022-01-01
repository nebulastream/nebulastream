/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Exceptions/InvalidFieldException.hpp>
#include <Util/Logger.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <utility>

namespace NES::Windowing {

WindowType::WindowType(TimeCharacteristicPtr timeCharacteristic) : timeCharacteristic(std::move(timeCharacteristic)) {}

TimeCharacteristicPtr WindowType::getTimeCharacteristic() const { return this->timeCharacteristic; }

bool WindowType::isSlidingWindow() { return false; }

bool WindowType::isTumblingWindow() { return false; }

bool WindowType::inferStamp(const SchemaPtr& schema) {
    if (timeCharacteristic->getType() == TimeCharacteristic::EventTime) {
        auto fieldName = timeCharacteristic->getField()->getName();
        auto existingField = schema->hasFieldName(fieldName);
        if (existingField) {
            timeCharacteristic->getField()->setName(existingField->getName());
            return false;
        }

        NES_ERROR("TumblingWindow using a non existing time field " + fieldName);
        throw InvalidFieldException("TumblingWindow using a non existing time field " + fieldName);
    }
    return true;
}

}// namespace NES::Windowing
