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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_BOOLEANSTAMP_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_BOOLEANSTAMP_HPP_
#include <Nautilus/IR/Types/Stamp.hpp>
#include <cstdint>
namespace NES::Nautilus::IR::Types {

class BooleanStamp : public Stamp {
  public:
    static const inline auto type = TypeIdentifier::create<BooleanStamp>();
    /**
     * @brief Constructor to create a boolean stamp.
     */
    BooleanStamp();
    const std::string toString() const override;
};

}// namespace NES::Nautilus::IR::Types

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_BOOLEANSTAMP_HPP_
