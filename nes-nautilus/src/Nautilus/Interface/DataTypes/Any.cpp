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
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>

namespace NES::Nautilus {

Any::Any(const TypeIdentifier* identifier) : Typed(identifier){};

Nautilus::IR::Types::StampPtr Any::getType() const { return Nautilus::IR::Types::StampFactory::createVoidStamp(); }

std::string Any::toString() { return "Any"; }

}// namespace NES::Nautilus