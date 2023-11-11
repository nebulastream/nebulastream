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

#ifndef NES_EXECUTION_OPERATORS_RELATIONAL_PYTHONPYPYUDFUTILS_HPP
#define NES_EXECUTION_OPERATORS_RELATIONAL_PYTHONPYPYUDFUTILS_HPP

#include <Nautilus/Util/Dyncall.hpp>

namespace NES::Runtime::Execution::Operators {

template<typename T>
T executePyPy(void* state);
bool executeToBooleanPyPy(void* state);
float executeToFloatPyPy(void* state);
double executeToDoublePyPy(void* state);
int8_t executeToInt8PyPy(void* state);
int16_t executeToInt16PyPy(void* state);
int32_t executeToInt32PyPy(void* state);
int64_t executeToInt64PyPy(void* state);
};// namespace NES::Runtime::Execution::Operators

#endif//NES_EXECUTION_OPERATORS_RELATIONAL_PYTHONPYPYUDFUTILS_HPP