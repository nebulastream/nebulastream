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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_ARRAYSTAMP_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_ARRAYSTAMP_HPP_

#include <Nautilus/IR/Types/Stamp.hpp>
#include <cstdint>
namespace NES::Nautilus::IR::Types {

/**
 * @brief A array stamp that represents a array data type of a fixed size and a specific component type.
 */
class ArrayStamp : public Stamp {
  public:
    static const inline auto type = TypeIdentifier::create<ArrayStamp>();
    /**
     * @brief Constructor to create a array stamp.
     */
    ArrayStamp(const uint64_t size, StampPtr componentStamp);

    uint64_t getSize() const;

    const StampPtr& getComponentStamp() const;
    const std::string toString() const override;

  private:
    const uint64_t size;
    const StampPtr componentStamp;
};

}// namespace NES::Nautilus::IR::Types

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_ARRAYSTAMP_HPP_
