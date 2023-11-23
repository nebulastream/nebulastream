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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_BASICTYPES_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_BASICTYPES_HPP_

#include <nlohmann/json.hpp>
#include <stdint.h>
#include <typeindex>

namespace NES {

enum class BasicType : uint8_t {
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    FLOAT32,
    UINT64,
    FLOAT64,
    BOOLEAN,
    CHAR,
    TEXT
};

NLOHMANN_JSON_SERIALIZE_ENUM(BasicType,
                             {
                                 {BasicType::INT8, "INT8"},
                                 {BasicType::UINT8, "UINT8"},
                                 {BasicType::INT16, "INT16"},
                                 {BasicType::UINT16, "UINT16"},
                                 {BasicType::INT32, "INT32"},
                                 {BasicType::UINT32, "UINT32"},
                                 {BasicType::INT64, "INT64"},
                                 {BasicType::FLOAT32, "FLOAT32"},
                                 {BasicType::UINT64, "UINT64"},
                                 {BasicType::FLOAT64, "FLOAT64"},
                                 {BasicType::BOOLEAN, "BOOLEAN"},
                                 {BasicType::CHAR, "CHAR"},
                                 {BasicType::TEXT, "TEXT"},
                             })

}// namespace NES

// TODO: if type is not found, returns first one (INT8)
// TODO: is incomplete
static std::unordered_map<std::type_index, NES::BasicType> TypeMap{
    {std::type_index(typeid(int8_t)), NES::BasicType::INT8},
    {std::type_index(typeid(uint8_t)), NES::BasicType::UINT8},
    {std::type_index(typeid(int16_t)), NES::BasicType::INT16},
    {std::type_index(typeid(uint16_t)), NES::BasicType::UINT16},
    {std::type_index(typeid(int32_t)), NES::BasicType::INT32},
    {std::type_index(typeid(uint32_t)), NES::BasicType::UINT32},
    {std::type_index(typeid(int64_t)), NES::BasicType::INT64},
    {std::type_index(typeid(float)), NES::BasicType::FLOAT32},
    {std::type_index(typeid(uint64_t)), NES::BasicType::UINT64},
    {std::type_index(typeid(size_t)), NES::BasicType::UINT64},
    {std::type_index(typeid(double)), NES::BasicType::FLOAT64},
    {std::type_index(typeid(bool)), NES::BasicType::BOOLEAN},
    {std::type_index(typeid(char)), NES::BasicType::CHAR},
    {std::type_index(typeid(std::string)), NES::BasicType::TEXT},
    {std::type_index(typeid(char[])), NES::BasicType::TEXT},
};

#endif// NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_BASICTYPES_HPP_
