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

namespace NES::Nautilus {

template<typename T>
concept IntegerTypes = std::same_as<T, int8_t> || std::same_as<T, int16_t> || std::same_as<T, int32_t> || std::same_as<T, uint8_t>
    || std::same_as<T, uint16_t> || std::same_as<T, uint32_t> || std::same_as<T, uint64_t> || std::same_as<T, double>
    || std::same_as<T, float>;

template<typename T>
concept DoubleTypes = std::same_as<T, int8_t> || std::same_as<T, int16_t> || std::same_as<T, int32_t> || std::same_as<T, uint8_t>
    || std::same_as<T, uint16_t> || std::same_as<T, uint32_t> || std::same_as<T, uint64_t> || std::same_as<T, double>
    || std::same_as<T, float>;

template<typename T>
concept FixedDataTypes = IntegerTypes<T> || DoubleTypes<T> || std::same_as<T, bool>;

template<typename T>
concept ConstantAndFixedDataTypes = std::same_as<T, int> || std::same_as<T, uint> || FixedDataTypes<T>;

} // namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPESCONCEPTS_HPP_
