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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_INTEGERSTAMP_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_INTEGERSTAMP_HPP_
#include <Nautilus/IR/Types/Stamp.hpp>
#include <cstdint>

namespace NES::Nautilus::IR::Types {

/**
 * @brief A integer stamp represents the type of an integer value.
 * It is defined by of a bit width and a signedness.
 */
class IntegerStamp : public Stamp {
  public:
    // Bit width for the integer
    enum class BitWidth : uint8_t { I8, I16, I32, I64 };

    // Signedness semantics.
    enum class SignednessSemantics : uint8_t {
        Signed,  /// Signed integer
        Unsigned,/// Unsigned integer
    };

    static const inline auto type = TypeIdentifier::create<IntegerStamp>();

    /**
     * @brief Constructor to create a integer stamp.
     * @param bitWidth defines the width of the integer, can be 8, 16, 32, and 64bit.
     * @param signedness defines the signedness of the integer, can be signed and unsigned.
     */
    IntegerStamp(BitWidth bitWidth, SignednessSemantics signedness);

    /**
     * @brief Returns the bit width of the integer.
     * @return BitWidth
     */
    BitWidth getBitWidth() const;

    /**
     * @brief Returns the number of bit as a uint32_t a value of this stamp will occupy.
     * @return uint32_t
     */
    uint32_t getNumberOfBits() const;

    /**
     * @brief Returns the signedness of this integer stamp.
     * @return SignednessSemantics
     */
    SignednessSemantics getSignedness() const;

    /**
     * @return true if integer is signed.
     */
    bool isSigned() const;

    /**
     * @return true if integer is unsigned.
     */
    bool isUnsigned() const;
    const std::string toString() const override;

  private:
    const BitWidth bitWidth;
    const SignednessSemantics signedness;
};

}// namespace NES::Nautilus::IR::Types

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_INTEGERSTAMP_HPP_
