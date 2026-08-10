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
#include <DataTypes/VarVal.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
class DefaultDateValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultDateValueSerializer(const bool quoted) : quoted(quoted) { }

    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<SerializerKey, std::string>& serializerTypes,
        const DataType& valueType) const override;

private:
    bool quoted;
};

class DefaultTimeValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultTimeValueSerializer(const bool quoted) : quoted(quoted) { }

    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<SerializerKey, std::string>& serializerTypes,
        const DataType& valueType) const override;

private:
    bool quoted;
};

class DefaultTimestampValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultTimestampValueSerializer(const bool quoted) : quoted(quoted) { }

    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<SerializerKey, std::string>& serializerTypes,
        const DataType& valueType) const override;

private:
    bool quoted;
};
}
