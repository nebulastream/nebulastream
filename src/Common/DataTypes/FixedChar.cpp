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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/FixedChar.hpp>

namespace NES {

FixedChar::FixedChar(uint64_t length) : length(length) {}

uint64_t FixedChar::getLength() const {
    return length;
}

bool FixedChar::isFixedChar() {
    return true;
}
bool FixedChar::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isFixedChar()) {
        auto otherChar = as<FixedChar>(otherDataType);
        return length == otherChar->getLength();
    }
    return false;
}

DataTypePtr FixedChar::join(DataTypePtr) {
    return DataTypeFactory::createUndefined();
}

std::string FixedChar::toString() {
    return "Char";
}

}// namespace NES