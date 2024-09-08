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
#include <Execution/Functions/ConstantValueFunction.hpp>
namespace NES::Runtime::Execution::Functions
{

template <typename T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
ConstantValueFunction<T>::ConstantValueFunction(T value) : value(value)
{
}

template <typename T>
requires std::is_integral_v<T> || std::is_floating_point_v<T>
VarVal ConstantValueFunction<T>::execute(Record&) const
{
    return VarVal(value);
}

template class ConstantValueFunction<int8_t>;
template class ConstantValueFunction<int16_t>;
template class ConstantValueFunction<int32_t>;
template class ConstantValueFunction<int64_t>;
template class ConstantValueFunction<uint8_t>;
template class ConstantValueFunction<uint16_t>;
template class ConstantValueFunction<uint32_t>;
template class ConstantValueFunction<uint64_t>;
template class ConstantValueFunction<float>;
template class ConstantValueFunction<bool>;
template class ConstantValueFunction<double>;

} /// namespace NES::Runtime::Execution::Functions
