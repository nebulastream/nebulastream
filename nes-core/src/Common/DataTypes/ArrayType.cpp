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

#include <Common/DataTypes/ArrayType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <utility>

namespace NES {

bool ArrayType::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isArray()) {
        auto const otherArray = as<ArrayType>(otherDataType);
        return length == otherArray->length && component->isEquals(otherArray->component);
    }
    return false;
}

DataTypePtr ArrayType::join(DataTypePtr) { return DataTypeFactory::createUndefined(); }

std::string ArrayType::toString() { return "ArrayType"; }

}// namespace NES