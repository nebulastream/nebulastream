/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_COMMON_DATATYPES_FIXEDCHAR_HPP_
#define NES_INCLUDE_COMMON_DATATYPES_FIXEDCHAR_HPP_

#include <Common/DataTypes/ArrayType.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES {

/**
 * @brief FixedChar(n), where n is a positive integer.
 * FixedChar can store strings up to n characters (not bytes) in length.
 * If the string to be stored is shorter than the declared length, values of type FixedChar will be space-padded.
 */
class FixedChar final : public ArrayType {
  public:
    /**
     * @brief Constructs a new Char(n)
     * @param length length of the char
     */
    explicit FixedChar(uint64_t length) noexcept;

    virtual ~FixedChar() = default;
};

}// namespace NES

#endif  // NES_INCLUDE_COMMON_DATATYPES_FIXEDCHAR_HPP_
