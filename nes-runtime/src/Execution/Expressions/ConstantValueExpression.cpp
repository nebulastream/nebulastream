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
#include <Execution/Expressions/ConstantValueExpression.hpp>
namespace NES::Runtime::Execution::Expressions {

template<typename T>
    requires std::is_integral_v<T> || std::is_floating_point_v<T>
ConstantValueExpression<T>::ConstantValueExpression(T value) : value(value) {}

template<typename T>
    requires std::is_integral_v<T> || std::is_floating_point_v<T>
Value<> ConstantValueExpression<T>::execute(Record&) const {
    return Value<>(value);
}

template class ConstantValueExpression<int8_t>;
template class ConstantValueExpression<int16_t>;
template class ConstantValueExpression<int32_t>;
template class ConstantValueExpression<int64_t>;
template class ConstantValueExpression<uint8_t>;
template class ConstantValueExpression<uint16_t>;
template class ConstantValueExpression<uint32_t>;
template class ConstantValueExpression<uint64_t>;
template class ConstantValueExpression<float>;
template class ConstantValueExpression<bool>;
template class ConstantValueExpression<double>;

}// namespace NES::Runtime::Execution::Expressions