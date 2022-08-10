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

Int32::Int32(int32_t value) : Int(&type), value(value){};
IR::Types::StampPtr Int32::getType() const { return IR::Types::StampFactory::createInt32Stamp(); }
std::unique_ptr<Any> Int32::copy() { return create<Int32>(value); }
const std::unique_ptr<Int> Int32::add(const Int& other) const {
    auto& otherValue = other.staticCast<Int32>();
    return create<Int32>(value + otherValue.value);
}

const std::unique_ptr<Int> Int32::sub(const Int& other) const {
    auto& otherValue = other.staticCast<Int32>();
    return create<Int32>(value - otherValue.value);
}
const std::unique_ptr<Int> Int32::div(const Int& other) const {
    auto& otherValue = other.staticCast<Int32>();
    return create<Int32>(value / otherValue.value);
}
const std::unique_ptr<Int> Int32::mul(const Int& other) const {
    auto& otherValue = other.staticCast<Int32>();
    return create<Int32>(value * otherValue.value);
}
const std::unique_ptr<Boolean> Int32::equals(const Int& other) const {
    auto& otherValue = other.staticCast<Int32>();
    return create<Boolean>(value == otherValue.value);
}
const std::unique_ptr<Boolean> Int32::lessThan(const Int& other) const {
    auto& otherValue = other.staticCast<Int32>();
    return create<Boolean>(value < otherValue.value);
}
const std::unique_ptr<Boolean> Int32::greaterThan(const Int& other) const {
    auto& otherValue = other.staticCast<Int32>();
    return create<Boolean>(value < otherValue.value);
}

int32_t Int32::getValue() const { return value; }
int64_t Int32::getRawInt() const { return value; }
}// namespace NES::ExecutionEngine::Experimental::Interpreter