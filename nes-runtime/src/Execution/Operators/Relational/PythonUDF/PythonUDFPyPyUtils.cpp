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

#include <Execution/Operators/Relational/PythonUDF/PythonUDFPyPyUtils.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <filesystem>
#include <fstream>
#include <dlfcn.h>

namespace NES::Runtime::Execution::Operators {

template<typename T>
T executePyPy(void* state){
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();

    void *udfSO = dlopen("/home/phm98/nebulastream/nebulastream/cmake-build-debug/nes-runtime/benchmark/tmp/udfs.so", RTLD_LAZY | RTLD_DEEPBIND);
    if(udfSO == NULL){
        NES_THROW_RUNTIME_ERROR("Could not load udfs.so", dlerror());
    }
    //int hello = (int)dlsym( lib, "integer_test" );
    auto functionAddress = dlsym(udfSO, handler->getFunctionName().c_str());

    // there is no string bc dyncall wrapper doesn't have a string function
    T result;
    if constexpr (std::is_same<T, bool>::value) {
        result = dyncall.callB((void*) functionAddress);
    } else if constexpr (std::is_same<T, float>::value) {
        result = dyncall.callF((void*) functionAddress);
    }  else if constexpr (std::is_same<T, double>::value) {
        result = dyncall.callD((void*) functionAddress);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        result = dyncall.callI64((void*) functionAddress);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        result = dyncall.callI32((void*) functionAddress);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        result = dyncall.callI16((void*) functionAddress);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        result = dyncall.callI8((void*) functionAddress);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    // reset all variables for next call
    dyncall.reset();
    return result;
}

bool executeToBooleanPyPy(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executePyPy<bool>(state);
}

float executeToFloatPyPy(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executePyPy<float>(state);
}

double executeToDoublePyPy(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executePyPy<double>(state);
}

int8_t executeToInt8PyPy(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executePyPy<int8_t>(state);
}

int16_t executeToInt16PyPy(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executePyPy<int16_t>(state);
}

int32_t executeToInt32PyPy(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executePyPy<int32_t>(state);
}

int64_t executeToInt64PyPy(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executePyPy<int64_t>(state);
}


}// namespace NES::Runtime::Execution::Operators