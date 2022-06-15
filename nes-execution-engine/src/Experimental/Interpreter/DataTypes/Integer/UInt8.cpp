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
#include <Experimental/Interpreter/DataValue/Integer/Int.hpp>
#include <Experimental/Interpreter/DataValue/InvocationPlugin.hpp>
#include <Experimental/NESIR/Types/IntegerStamp.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

UInt8::UInt8(uint8_t value) : Int(&type), value(value){};
IR::Types::StampPtr UInt8::getType() const { return IR::Types::StampFactory::createUInt8Stamp(); }
std::unique_ptr<Any> UInt8::copy() { return create<UInt8>(value); }
const std::unique_ptr<Int> UInt8::add(const Int& other) const {
    auto& otherValue = other.staticCast<UInt8>();
    return create<UInt8>(value + otherValue.value);
}

const std::unique_ptr<Int> UInt8::sub(const Int& other) const {
    auto& otherValue = other.staticCast<UInt8>();
    return create<UInt8>(value - otherValue.value);
}
const std::unique_ptr<Int> UInt8::div(const Int& other) const {
    auto& otherValue = other.staticCast<UInt8>();
    return create<UInt8>(value / otherValue.value);
}
const std::unique_ptr<Int> UInt8::mul(const Int& other) const {
    auto& otherValue = other.staticCast<UInt8>();
    return create<UInt8>(value * otherValue.value);
}
const std::unique_ptr<Boolean> UInt8::equals(const Int& other) const {
    auto& otherValue = other.staticCast<UInt8>();
    return create<Boolean>(value == otherValue.value);}

const std::unique_ptr<Boolean> UInt8::lessThan(const Int& other) const {
    auto& otherValue = other.staticCast<UInt8>();
    return create<Boolean>(value > otherValue.value);}

uint8_t UInt8::getValue() const { return value; }

int64_t UInt8::getRawInt() const { return value; }
}// namespace NES::ExecutionEngine::Experimental::Interpreter