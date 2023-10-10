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

#ifndef NES_EXECUTION_OPERATORS_RELATIONAL_PYTHONNUMBAUDFUTILS_HPP
#define NES_EXECUTION_OPERATORS_RELATIONAL_PYTHONNUMBAUDFUTILS_HPP

#include <Nautilus/Util/Dyncall.hpp>

namespace NES::Runtime::Execution::Operators {
void createBooleanNumba(void* state, bool value);
void createFloatNumba(void* state, float value);
void createDoubleNumba(void* state, double value);
void createInt8Numba(void* state, int8_t value);
void createInt16Numba(void* state, int16_t value);
void createInt32Numba(void* state, int32_t value);
void createInt64Numba(void* state, int64_t value);

template<typename T>
T executeNumba(void* state);
bool executeToBoolean(void* state);
float executeToFloat(void* state);
double executeToDouble(void* state);
int8_t executeToInt8(void* state);
int16_t executeToInt16(void* state);
int32_t executeToInt32(void* state);
int64_t executeToInt64(void* state);
};// namespace NES::Runtime::Execution::Operators

#endif//NES_EXECUTION_OPERATORS_RELATIONAL_PYTHONNUMBAUDFUTILS_HPP