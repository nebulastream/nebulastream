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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_FLOATSTAMP_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_FLOATSTAMP_HPP_
#include <Nautilus/IR/Types/Stamp.hpp>
#include <cstdint>
namespace NES::Nautilus::IR::Types {

class FloatStamp : public Stamp {
  public:
    // Bit width for the float
    enum class BitWidth : uint32_t { F32, F64 };

    static const inline auto type = TypeIdentifier::create<FloatStamp>();
    /**
     * @brief Constructor to create a float stamp.
     * @param bitWidth defines the width of the float, can be 32, and 64bit.
     */
    FloatStamp(BitWidth bitWidth);

    /**
     * @brief Returns the bit width of the float.
     * @return BitWidth
     */
    BitWidth getBitWidth() const;

    /**
     * @brief Returns the number of bit as a uint32_t a value of this stamp will occupy.
     * @return uint32_t
     */
    uint32_t getNumberOfBits() const;
    const std::string toString() const override;

  private:
    const BitWidth bitWidth;
};

}// namespace NES::Nautilus::IR::Types

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_FLOATSTAMP_HPP_
