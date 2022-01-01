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

#include <Common/DataTypes/DataType.hpp>

namespace NES {

bool DataType::isBoolean() const { return false; }

bool DataType::isUndefined() const { return false; }

bool DataType::isArray() const { return false; }

bool DataType::isCharArray() const { return false; }

bool DataType::isFloat() const { return false; }

bool DataType::isInteger() const { return false; }

bool DataType::isNumeric() const { return false; }

bool DataType::isChar() const { return false; }

}// namespace NES