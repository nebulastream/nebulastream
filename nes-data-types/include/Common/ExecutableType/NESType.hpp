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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_EXECUTABLETYPE_NESTYPE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_EXECUTABLETYPE_NESTYPE_HPP_

#include <cstdint>
#include <string>
#include <type_traits>

namespace NES {
/**
 * @brief Base class for all nes specific data types
 */
class NESType {};

template<class Type>
concept IsNesType =
    std::is_fundamental_v<Type> || std::is_fundamental_v<std::remove_pointer_t<Type>> || std::is_base_of_v<NESType, Type>;

/**
 * @brief This concept checks via tuple unpacking if Types contains at least one string.
 * @tparam Types
 */
template<class... Types>
concept ContainsString = requires { requires(std::is_same_v<std::string, Types> || ...); };

template<class Type>
concept IsInt8 = std::is_same_v<std::remove_cvref_t<Type>, std::int8_t>;
template<class Type>
concept IsInt16 = std::is_same_v<std::remove_cvref_t<Type>, std::int16_t>;
template<class Type>
concept IsInt32 = std::is_same_v<std::remove_cvref_t<Type>, std::int32_t>;
template<class Type>
concept IsInt64 = std::is_same_v<std::remove_cvref_t<Type>, std::int64_t>;
template<class Type>
concept IsUInt8 = std::is_same_v<std::remove_cvref_t<Type>, std::uint8_t>;
template<class Type>
concept IsUInt16 = std::is_same_v<std::remove_cvref_t<Type>, std::uint16_t>;
template<class Type>
concept IsUInt32 = std::is_same_v<std::remove_cvref_t<Type>, std::uint32_t>;
template<class Type>
concept IsUInt64 = std::is_same_v<std::remove_cvref_t<Type>, std::uint64_t>;
template<class Type>
concept IsFloat = std::is_same_v<std::remove_cvref_t<Type>, float>;
template<class Type>
concept IsDouble = std::is_same_v<std::remove_cvref_t<Type>, double>;
template<class Type>
concept IsChar = std::is_same_v<std::remove_cvref_t<Type>, char>;
template<class Type>
concept IsBool = std::is_same_v<std::remove_cvref_t<Type>, bool>;
template<class Type>
concept IsString = std::is_same_v<std::remove_cvref_t<Type>, std::string>;
template<class Type>
concept IsArray = std::is_fundamental_v<std::remove_pointer_t<Type>> || std::is_base_of_v<NESType, Type>;

}// namespace NES
#endif // NES_DATA_TYPES_INCLUDE_COMMON_EXECUTABLETYPE_NESTYPE_HPP_
