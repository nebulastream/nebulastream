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
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/LazyValueData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ValueDeserializer.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Current placeholder for a fully functional ValueDeserializerDescriptor.
/// Attributes are either passed into the deserializer implementation or used in the ValueDeserializerProvider to determine the deserializer type.
struct ValueDeserializerConfig
{
    bool nullable;
    bool quoted;
    bool hasTrailingSpaces;
};

/// Resolves the deserializer that the user configured for individual fields against the fields of the schema.
/// The function expects the overrides string to be formatted like this: [FIELD-NAME]:[DESERIALIZER-KEY],...
/// Throws an InvalidConfigParameter for a malformed entry or an entry that names no field of the schema.
[[nodiscard]] std::unordered_map<Record::RecordFieldIdentifier, std::string>
parseValueDeserializerOverrides(const std::string& overrides, const std::vector<Record::RecordFieldIdentifier>& fieldNames);

/// Fetches ValueDeserializer from Registry
/// The concrete type of deserializer is [Nullable]<deserializerType>ValueDeserializer
std::unique_ptr<ValueDeserializer> provideValueDeserializer(const std::string& deserializerType, const ValueDeserializerConfig& config);

/// Implementation of the LazyValueDeserializer interface defined in the LazyValueData file.
/// It fetches the configured ValueDeserializer from the registry and deserializes the field on demand.
class LazyFieldDeserializer final : public LazyValueDeserializer
{
public:
    /// The null values are owned by the indexer, which outlives every trace that reads records with it.
    LazyFieldDeserializer(
        const std::string& deserializerKey,
        const ValueDeserializerConfig& config,
        const std::vector<std::string>& nullValues,
        const nautilus::val<Arena*>& arena);

    [[nodiscard]] VarVal deserialize(const nautilus::val<int8_t*>& ptr, const nautilus::val<uint64_t>& size) const override;

private:
    std::unique_ptr<ValueDeserializer> valueDeserializer;
    const std::vector<std::string>* nullValues;
    nautilus::val<Arena*> arena;
};

/// Creates a VarVal that holds a serialized value as LazyValueData. IsNull is not known before deserialization, so the VarVal
/// carries "not null" as placeholder, which VarVal resolves on demand by deserializing the value.
[[nodiscard]] VarVal createLazyFieldValue(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::string& deserializerKey,
    const ValueDeserializerConfig& config,
    const std::vector<std::string>& nullValues,
    const ArenaRef& arena);
}
