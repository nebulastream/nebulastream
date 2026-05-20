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

class JSONSTRUCTValueDeserializer final : public ValueDeserializer
{
public:
    explicit JSONSTRUCTValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        ArenaRef& arena) const override;


    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        ArenaRef& arena,
        const nautilus::val<int8_t*>& bufferAddress) const override;
};

class JSONFIXEDSIZEDValueDeserializer final : public ValueDeserializer
{
public:
    explicit JSONFIXEDSIZEDValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        ArenaRef& arena) const override;


    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        ArenaRef& arena,
        const nautilus::val<int8_t*>& bufferAddress) const override;
};

class JSONVARARRAYValueDeserializer final : public ValueDeserializer
{
public:
    explicit JSONVARARRAYValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        ArenaRef& arena) const override;


    void deserializeIntoBuffer(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
        const DataType& valueType,
        ArenaRef& arena,
        const nautilus::val<int8_t*>& bufferAddress) const override;
};
}
