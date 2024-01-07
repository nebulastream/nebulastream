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
#include <Nautilus/IR/Types/AddressStamp.hpp>
#include <Nautilus/IR/Types/ArrayStamp.hpp>
#include <Nautilus/IR/Types/BooleanStamp.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/IR/Types/Stamp.hpp>
#include <Nautilus/IR/Types/VoidStamp.hpp>

namespace NES::Nautilus::IR::Types {

Stamp::Stamp(const TypeIdentifier* typeIdentifier) : Typed(typeIdentifier) {}

bool Stamp::isInteger() { return isa<IntegerStamp>(*this); }

bool Stamp::isFloat() { return isa<FloatStamp>(*this); }

bool Stamp::isArray() { return isa<ArrayStamp>(*this); }
bool Stamp::isAddress() { return isa<AddressStamp>(*this); }

bool Stamp::isBoolean() { return isa<BooleanStamp>(*this); }

bool Stamp::isVoid() { return isa<VoidStamp>(*this); }

}// namespace NES::Nautilus::IR::Types