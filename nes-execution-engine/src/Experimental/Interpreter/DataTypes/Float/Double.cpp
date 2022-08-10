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
#include <Experimental/Interpreter/DataValue/Float/Double.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

Double::Double(double value) : TraceableType(&type), value(value){};
IR::Types::StampPtr Double::getType() const { return IR::Types::StampFactory::createDoubleStamp(); }
std::unique_ptr<Any> Double::copy() { return create<Double>(value); }
std::unique_ptr<Double> Double::add(const Double& otherValue) const { return create<Double>(value + otherValue.value); }
std::unique_ptr<Double> Double::sub(const Double& otherValue) const { return create<Double>(value - otherValue.value); }
std::unique_ptr<Double> Double::mul(const Double& otherValue) const { return create<Double>(value * otherValue.value); }
std::unique_ptr<Double> Double::div(const Double& otherValue) const { return create<Double>(value / otherValue.value); }
std::unique_ptr<Boolean> Double::equals(const Double& otherValue) const { return create<Boolean>(value == otherValue.value); }
std::unique_ptr<Boolean> Double::lessThan(const Double& otherValue) const { return create<Boolean>(value < otherValue.value); }
std::unique_ptr<Boolean> Double::greaterThan(const Double& otherValue) const { return create<Boolean>(value > otherValue.value); }
double Double::getValue() const { return value; }

}// namespace NES::ExecutionEngine::Experimental::Interpreter