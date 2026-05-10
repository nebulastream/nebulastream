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

#include <DataTypes/VarVal.hpp>
#include <Arena.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Deserializes a JSON string into the single character it encodes, decoding JSON escape sequences ('\n', '\uXXXX', ...) first.
class JSONCHARValueDeserializer final : public ValueDeserializer
{
public:
    explicit JSONCHARValueDeserializer() noexcept = default;

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType) const override;

    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        const nautilus::val<int8_t*>& bufferAddress) const override;

    static std::unique_ptr<ValueDeserializer> provideDeserializer(ValueDeserializerRegistryArguments args);
};

/// Deserializes a JSON string into its value, decoding JSON escape sequences ('\n', '\uXXXX', ...).
/// The raw JSON text of a string is not its value: '"ABC"' is a nine byte token that encodes the three bytes 'ABC'.
class JSONVARSIZEDValueDeserializer final : public ValueDeserializer
{
public:
    explicit JSONVARSIZEDValueDeserializer() noexcept = default;

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType) const override;

    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        const nautilus::val<int8_t*>& bufferAddress) const override;

    static std::unique_ptr<ValueDeserializer> provideDeserializer(ValueDeserializerRegistryArguments args);
};

class NullableJSONCHARValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableJSONCHARValueDeserializer() noexcept = default;

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType) const override;

    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        const nautilus::val<int8_t*>& bufferAddress) const override;

    static std::unique_ptr<ValueDeserializer> provideDeserializer(ValueDeserializerRegistryArguments args);
};

class NullableJSONVARSIZEDValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableJSONVARSIZEDValueDeserializer() noexcept = default;

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType) const override;

    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        const nautilus::val<int8_t*>& bufferAddress) const override;

    static std::unique_ptr<ValueDeserializer> provideDeserializer(ValueDeserializerRegistryArguments args);
};

/// Deserializer for json formatted structs in {"key": value, ...} form.
/// For simplicity, this PoC assumes that the fieldAddress points to a raw json struct within a simdjson document.
/// Therefore, it cannot not be used for any arbitrary json-formatted struct.
/// For these cases, a separate deserializer including a full json-struct parser should be written.
class JSONSTRUCTValueDeserializer final : public ValueDeserializer
{
public:
    explicit JSONSTRUCTValueDeserializer() noexcept = default;

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType) const override;

    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        const nautilus::val<int8_t*>& bufferAddress) const override;

    static std::unique_ptr<ValueDeserializer> provideDeserializer(ValueDeserializerRegistryArguments args);
};

/// Deserializer for json formatted arrays in [value, value, ...] form.
/// For simplicity, this PoC assumes that the fieldAddress points to a raw json array within a simdjson document.
/// Therefore, it cannot not be used for any arbitrary json-formatted struct.
/// For these cases, a separate deserializer including a full json-array parser should be written.
class JSONFIXEDSIZEDValueDeserializer final : public ValueDeserializer
{
public:
    explicit JSONFIXEDSIZEDValueDeserializer() noexcept = default;

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType) const override;

    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        const nautilus::val<int8_t*>& bufferAddress) const override;

    static std::unique_ptr<ValueDeserializer> provideDeserializer(ValueDeserializerRegistryArguments args);
};
}
