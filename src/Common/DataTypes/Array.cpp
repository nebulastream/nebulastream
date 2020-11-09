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


#include <Common/DataTypes/Array.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <utility>

namespace NES {

Array::Array(uint64_t length, DataTypePtr component) : length(length), component(std::move(component)) {}

DataTypePtr Array::getComponent() {
    return component;
}

uint64_t Array::getLength() const {
    return length;
}

bool Array::isArray() {
    return true;
}
bool Array::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isArray()) {
        auto otherArray = as<Array>(otherDataType);
        return length == otherArray->getLength() && component->isEquals(otherArray->getComponent());
    }
    return false;
}

DataTypePtr Array::join(DataTypePtr) {
    return DataTypeFactory::createUndefined();
}

std::string Array::toString() {
    return "Array";
}

}// namespace NES