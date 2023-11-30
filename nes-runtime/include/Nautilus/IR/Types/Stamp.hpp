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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_STAMP_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_STAMP_HPP_
#include <Nautilus/Util/CastUtils.hpp>
#include <memory>
#include <ostream>
#include <typeinfo>
namespace NES::Nautilus::IR::Types {

class Stamp : public Typed {
  public:
    Stamp(const TypeIdentifier* typeIdentifier);
    virtual ~Stamp() = default;
    bool isInteger();
    bool isFloat();
    bool isArray();
    bool isBoolean();
    bool isVoid();
    bool isAddress();

    virtual const std::string toString() const = 0;
};

using StampPtr = std::shared_ptr<Stamp>;

}// namespace NES::Nautilus::IR::Types

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_TYPES_STAMP_HPP_
