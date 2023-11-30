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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BOOLEAN_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BOOLEAN_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
namespace NES::Nautilus {

/**
 * @brief Boolean data type.
 */
class Boolean : public TraceableType {
  public:
    using RawType = bool;
    static const inline auto type = TypeIdentifier::create<Boolean>();

    Boolean(bool value);

    std::shared_ptr<Any> copy() override;

    operator bool() const;

    bool getValue() const;

    ~Boolean() {}
    Nautilus::IR::Types::StampPtr getType() const override;
    std::string toString() override;

  private:
    const bool value;
};

}// namespace NES::Nautilus

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_BOOLEAN_HPP_
