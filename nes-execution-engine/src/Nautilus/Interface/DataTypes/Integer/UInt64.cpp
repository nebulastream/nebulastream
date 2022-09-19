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
#include <Nautilus/Interface/DataValue/Integer/Int.hpp>
#include <Nautilus/Interface/DataValue/InvocationPlugin.hpp>
#include <Experimental/NESIR/Types/IntegerStamp.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

UInt64::UInt64(uint64_t value) : Int(&type), value(value){};
IR::Types::StampPtr UInt64::getType() const { return IR::Types::StampFactory::createUInt64Stamp(); }
std::unique_ptr<Any> UInt64::copy() { return create<UInt64>(value); }
const std::unique_ptr<Int> UInt64::add(const Int& other) const {
    auto& otherValue = other.staticCast<UInt64>();
    return create<UInt64>(value + otherValue.value);
}

const std::unique_ptr<Int> UInt64::sub(const Int& other) const {
    auto& otherValue = other.staticCast<UInt64>();
    return create<UInt64>(value - otherValue.value);
}
const std::unique_ptr<Int> UInt64::div(const Int& other) const {
    auto& otherValue = other.staticCast<UInt64>();
    return create<UInt64>(value / otherValue.value);
}
const std::unique_ptr<Int> UInt64::mul(const Int& other) const {
    auto& otherValue = other.staticCast<UInt64>();
    return create<UInt64>(value * otherValue.value);
}
const std::unique_ptr<Boolean> UInt64::equals(const Int& other) const {
    auto& otherValue = other.staticCast<UInt64>();
    return create<Boolean>(value == otherValue.value);
}
const std::unique_ptr<Boolean> UInt64::lessThan(const Int& other) const {
    auto& otherValue = other.staticCast<UInt64>();
    return create<Boolean>(value < otherValue.value);
}
const std::unique_ptr<Boolean> UInt64::greaterThan(const Int& other) const {
    auto& otherValue = other.staticCast<UInt64>();
    return create<Boolean>(value < otherValue.value);
}

uint64_t UInt64::getValue() const { return value; }
int64_t UInt64::getRawInt() const { return value; }
}// namespace NES::ExecutionEngine::Experimental::Interpreter