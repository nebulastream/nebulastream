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

Int16::Int16(int16_t value) : Int(&type), value(value){};
IR::Types::StampPtr Int16::getType() const { return IR::Types::StampFactory::createInt16Stamp(); }
std::unique_ptr<Any> Int16::copy() { return create<Int16>(value); }
const std::unique_ptr<Int> Int16::add(const Int& other) const {
    auto& otherValue = other.staticCast<Int16>();
    return create<Int16>(value + otherValue.value);
}

const std::unique_ptr<Int> Int16::sub(const Int& other) const {
    auto& otherValue = other.staticCast<Int16>();
    return create<Int16>(value - otherValue.value);
}
const std::unique_ptr<Int> Int16::div(const Int& other) const {
    auto& otherValue = other.staticCast<Int16>();
    return create<Int16>(value / otherValue.value);
}
const std::unique_ptr<Int> Int16::mul(const Int& other) const {
    auto& otherValue = other.staticCast<Int16>();
    return create<Int16>(value * otherValue.value);
}
const std::unique_ptr<Boolean> Int16::equals(const Int& other) const {
    auto& otherValue = other.staticCast<Int16>();
    return create<Boolean>(value == otherValue.value);
}
const std::unique_ptr<Boolean> Int16::lessThan(const Int& other) const {
    auto& otherValue = other.staticCast<Int16>();
    return create<Boolean>(value < otherValue.value);
}
const std::unique_ptr<Boolean> Int16::greaterThan(const Int& other) const {
    auto& otherValue = other.staticCast<Int16>();
    return create<Boolean>(value < otherValue.value);
}

int16_t Int16::getValue() const { return value; }
int64_t Int16::getRawInt() const { return value; }
}// namespace NES::ExecutionEngine::Experimental::Interpreter