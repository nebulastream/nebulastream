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

#include <cstdint>
#include <string>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <val_ptr.hpp>
#include <DataTypes/DataType.hpp>

namespace NES::Nautilus
{

/// Encodes a single Record to a contiguous binary row according to the given schema.
/// For fixed-size types, writes native little-endian bytes of the appropriate width.
/// For variable-sized, writes [u32 length][bytes...] where length includes only content length.
class BinaryRowEncoder
{
public:
    explicit BinaryRowEncoder(const Schema& schema);

    nautilus::val<uint32_t> computeSize(Record& record) const;

    void encodeTo(Record& record, const nautilus::val<int8_t*>& dst) const;

private:
    struct FieldInfo
    {
        std::string name;
        DataType type;
        uint32_t fixedSize; /// 0 for var-sized
    };

    std::vector<FieldInfo> fields;
};

}
