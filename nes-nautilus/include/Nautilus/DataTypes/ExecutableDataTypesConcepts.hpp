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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPESCONCEPTS_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPESCONCEPTS_HPP_

#include <cstdint>
#include <sys/types.h>
#include <concepts>
namespace NES::Nautilus {

template<typename T>
concept IntegerTypes = std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
    std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>;

template<typename T>
concept DoubleTypes = std::is_same_v<T, double> || std::is_same_v<T, float>;

template<typename T>
concept FixedDataTypes = IntegerTypes<T> || DoubleTypes<T> || std::is_same_v<T, bool>;

template<typename T>
concept ConstantAndFixedDataTypes = std::is_same_v<T, int> || std::is_same_v<T, uint> || FixedDataTypes<T>;

} // namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPESCONCEPTS_HPP_
