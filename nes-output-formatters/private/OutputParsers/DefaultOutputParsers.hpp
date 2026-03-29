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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputParser.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
class DefaultBOOLOutputParser final : public OutputParser
{
public:
    explicit DefaultBOOLOutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultCHAROutputParser final : public OutputParser
{
public:
    explicit DefaultCHAROutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultF32OutputParser final : public OutputParser
{
public:
    explicit DefaultF32OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultF64OutputParser final : public OutputParser
{
public:
    explicit DefaultF64OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultINT8OutputParser final : public OutputParser
{
public:
    explicit DefaultINT8OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultINT16OutputParser final : public OutputParser
{
public:
    explicit DefaultINT16OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultINT32OutputParser final : public OutputParser
{
public:
    explicit DefaultINT32OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultINT64OutputParser final : public OutputParser
{
public:
    explicit DefaultINT64OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultUINT8OutputParser final : public OutputParser
{
public:
    explicit DefaultUINT8OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultUINT16OutputParser final : public OutputParser
{
public:
    explicit DefaultUINT16OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultUINT32OutputParser final : public OutputParser
{
public:
    explicit DefaultUINT32OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultUINT64OutputParser final : public OutputParser
{
public:
    explicit DefaultUINT64OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultVARSIZEDOutputParser final : public OutputParser
{
public:
    explicit DefaultVARSIZEDOutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class QuotedVARSIZEDOutputParser final : public OutputParser
{
public:
    explicit QuotedVARSIZEDOutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultFIXEDSIZEDOutputParser final : public OutputParser
{
public:
    explicit DefaultFIXEDSIZEDOutputParser() noexcept = default;
    nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultSTRUCTOutputParser final : public OutputParser
{
public:
    explicit DefaultSTRUCTOutputParser() noexcept = default;
    nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};

class DefaultUNDEFINEDOutputParser final : public OutputParser
{
public:
    explicit DefaultUNDEFINEDOutputParser() noexcept = default;
    nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress,
        const std::unordered_map<DataType::Type, std::string>& parserTypes,
        const DataType& valueType) const override;
};
}
