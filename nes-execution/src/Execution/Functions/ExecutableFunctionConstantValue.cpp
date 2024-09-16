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
#include <Execution/Functions/ExecutableFunctionConstantValue.hpp>
namespace NES::Runtime::Execution::Functions
{

template <typename T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
ExecutableFunctionConstantValue<T>::ExecutableFunctionConstantValue(T value) : value(value)
{
}

template <typename T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
VarVal ExecutableFunctionConstantValue<T>::execute(Record&) const
{
    return VarVal(value);
}

template class ExecutableFunctionConstantValue<int8_t>;
template class ExecutableFunctionConstantValue<int16_t>;
template class ExecutableFunctionConstantValue<int32_t>;
template class ExecutableFunctionConstantValue<int64_t>;
template class ExecutableFunctionConstantValue<uint8_t>;
template class ExecutableFunctionConstantValue<uint16_t>;
template class ExecutableFunctionConstantValue<uint32_t>;
template class ExecutableFunctionConstantValue<uint64_t>;
template class ExecutableFunctionConstantValue<float>;
template class ExecutableFunctionConstantValue<bool>;
template class ExecutableFunctionConstantValue<double>;

} /// namespace NES::Runtime::Execution::Functions
