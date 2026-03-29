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
#include <ValueSerializerRegistry.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
class DefaultBOOLValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultBOOLValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultCHARValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultCHARValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultF32ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultF32ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultF64ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultF64ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultINT8ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultINT8ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultINT16ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultINT16ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultINT32ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultINT32ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultINT64ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultINT64ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultUINT8ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultUINT8ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultUINT16ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultUINT16ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultUINT32ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultUINT32ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultUINT64ValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultUINT64ValueSerializer() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);
};

class DefaultVARSIZEDValueSerializer final : public ValueSerializer
{
public:
    explicit DefaultVARSIZEDValueSerializer(const bool& quoted) : quoted(quoted) { }

    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    static std::unique_ptr<ValueSerializer> provideSerializer(ValueSerializerRegistryArguments args);

private:
    bool quoted;
};
}
