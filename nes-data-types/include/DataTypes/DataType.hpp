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

#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

struct DataType final
{
    enum class Type : uint8_t
    {
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT32,
        FLOAT64,
        BOOLEAN,
        CHAR,
        UNDEFINED,
        VARSIZED,
    };

    bool operator==(const DataType& other) const = default;
    bool operator!=(const DataType& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const DataType& dataType);

    uint32_t getSizeInBytes() const;
    DataType join(const DataType& otherDataType);
    std::string formattedBytesToString(const void* data) const;

    [[nodiscard]] bool isInteger() const;
    [[nodiscard]] bool isSignedInteger() const;
    [[nodiscard]] bool isFloat() const;
    [[nodiscard]] bool isNumeric() const;
    [[nodiscard]] bool isBoolean() const;
    [[nodiscard]] bool isChar() const;
    [[nodiscard]] bool isVarSized() const;
    [[nodiscard]] bool isUndefined() const;

    Type type{Type::UNDEFINED};
};

}

FMT_OSTREAM(NES::DataType);
