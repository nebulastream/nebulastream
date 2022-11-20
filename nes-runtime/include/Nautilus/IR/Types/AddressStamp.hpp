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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_ADDRESSSTAMP_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_ADDRESSSTAMP_HPP_
#include <Nautilus/IR/Types/Stamp.hpp>
namespace NES::Nautilus::IR::Types {

/**
 * @brief A address stamp, which represents a data type for a memory address.
 */
class AddressStamp : public Stamp {
  public:
    static const inline auto type = TypeIdentifier::create<AddressStamp>();

    /**
     * @brief Constructor to create a void stamp.
     */
    AddressStamp();

    const std::string toString() const override;
};

}// namespace NES::Nautilus::IR::Types

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_ADDRESSSTAMP_HPP_
