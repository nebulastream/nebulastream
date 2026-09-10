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
#include <unordered_map>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Arena.hpp>
#include <ValueDeserializer.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Time arrives as a microseconds since unix epoch in textual form. Simply use the configured int64_t deserializer here, as we also use this representation internally.
class UnixEpochTimestampValueDeserializer final : public ValueDeserializer
{
public:
    explicit UnixEpochTimestampValueDeserializer(const bool quoted, const bool hasTrailingSpaces)
        : quoted(quoted), hasTrailingSpaces(hasTrailingSpaces)
    {
    }

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
        const DataType& valueType,
        ArenaRef& arena) const override;

    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
        const DataType& valueType,
        ArenaRef& arena,
        const nautilus::val<int8_t*>& bufferAddress) const override;

private:
    bool quoted;
    bool hasTrailingSpaces;
};
}
