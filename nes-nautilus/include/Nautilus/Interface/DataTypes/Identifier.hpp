//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        https://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

#ifndef NES_IDENTIFIER_HPP
#define NES_IDENTIFIER_HPP

#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
namespace NES::Nautilus {
/**
 * @brief Abstract integer data type.
 */
template<NESIdentifier T>
class Identifier final : public TraceableType {
  public:
    explicit Identifier(const T identifier) : value(identifier) {}
    T::Underlying getRawValue() const { return value.getRawValue(); }

  private:
    T value;
};
}// namespace NES::Nautilus
#endif//NES_IDENTIFIER_HPP
