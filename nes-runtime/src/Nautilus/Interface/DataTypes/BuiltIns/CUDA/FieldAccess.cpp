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

#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/FieldAccess.hpp>
#include <Nautilus/Interface/DataTypes/BuiltIns/CUDA/BlockDim.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus {

FieldAccess::FieldAccess(std::shared_ptr<BuiltInVariable> builtInVariable, std::string fieldName)
    : BuiltInVariable(&type)
    , builtInVariable(std::move(builtInVariable))
    , fieldName(std::move(fieldName))
{

};

const std::string FieldAccess::getIdentifier() const {
    return builtInVariable->getIdentifier() + "." + fieldName;
}

IR::Types::StampPtr FieldAccess::getType() const {
    // TODO Not always correct, as there are also vector types for float, double, signed ints
    return IR::Types::StampFactory::createUInt64Stamp();
}

std::shared_ptr<Any> FieldAccess::copy() {
    return create<FieldAccess>(builtInVariable, fieldName);
}

const Value<> FieldAccess::getAsValue() const {
    return Value<>((uint64_t)0);
}

} // namespace NES::Nautilus
