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
#include <Nautilus/Interface/DataTypes/Float/Double.hpp>

namespace NES::Nautilus {

Double::Double(double value) : TraceableType(&type), value(value){};
Nautilus::IR::Types::StampPtr Double::getType() const { return Nautilus::IR::Types::StampFactory::createDoubleStamp(); }
std::shared_ptr<Any> Double::copy() { return create<Double>(value); }
std::shared_ptr<Double> Double::add(const Double& otherValue) const { return create<Double>(value + otherValue.value); }
std::shared_ptr<Double> Double::sub(const Double& otherValue) const { return create<Double>(value - otherValue.value); }
std::shared_ptr<Double> Double::mul(const Double& otherValue) const { return create<Double>(value * otherValue.value); }
std::shared_ptr<Double> Double::div(const Double& otherValue) const { return create<Double>(value / otherValue.value); }
std::shared_ptr<Boolean> Double::equals(const Double& otherValue) const { return create<Boolean>(value == otherValue.value); }
std::shared_ptr<Boolean> Double::lessThan(const Double& otherValue) const { return create<Boolean>(value < otherValue.value); }
std::shared_ptr<Boolean> Double::greaterThan(const Double& otherValue) const { return create<Boolean>(value > otherValue.value); }
double Double::getValue() const { return value; }
std::string Double::toString() { return std::to_string(value); }

}// namespace NES::Nautilus