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

/// Assumes the dates to arrive in iso year-month-day format like 2026-08-12. Do not leave out leading zeros for month and day and represent years as 4 digits
class DefaultDateValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultDateValueDeserializer(const bool quoted, const bool hasTrailingSpaces)
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

/// Assumes the time to arrive in iso format like 22:50:44.123456. Do not leave out leading zeros for hour, minute and second
class DefaultTimeValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultTimeValueDeserializer(const bool quoted, const bool hasTrailingSpaces)
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

/// Expects a concatenation of a date and a time, like: 1999-01-08 04:05:06
class DefaultTimestampValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultTimestampValueDeserializer(const bool quoted, const bool hasTrailingSpaces)
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
