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
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>

namespace NES::Nautilus {

Float::Float(float value) : TraceableType(&type), value(value){};
Nautilus::IR::Types::StampPtr Float::getType() const { return Nautilus::IR::Types::StampFactory::createFloatStamp(); }
std::shared_ptr<Any> Float::copy() { return create<Float>(value); }
std::shared_ptr<Float> Float::add(const Float& otherValue) const { return create<Float>(value + otherValue.value); }
std::shared_ptr<Float> Float::sub(const Float& otherValue) const { return create<Float>(value - otherValue.value); }
std::shared_ptr<Float> Float::mul(const Float& otherValue) const { return create<Float>(value * otherValue.value); }
std::shared_ptr<Float> Float::div(const Float& otherValue) const { return create<Float>(value / otherValue.value); }
std::shared_ptr<Boolean> Float::equals(const Float& otherValue) const { return create<Boolean>(value == otherValue.value); }
std::shared_ptr<Boolean> Float::lessThan(const Float& otherValue) const { return create<Boolean>(value < otherValue.value); }
std::shared_ptr<Boolean> Float::greaterThan(const Float& otherValue) const { return create<Boolean>(value > otherValue.value); }
float Float::getValue() const { return value; }
std::string Float::toString() { return std::to_string(value); }

}// namespace NES::Nautilus