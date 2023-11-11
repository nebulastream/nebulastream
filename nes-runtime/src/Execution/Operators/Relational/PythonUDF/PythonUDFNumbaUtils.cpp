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

#include <Execution/Operators/Relational/PythonUDF/PythonUDFNumbaUtils.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <filesystem>
#include <fstream>

namespace NES::Runtime::Execution::Operators {

void createBooleanNumba(void* state, bool value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();
    dyncall.addArgB(value);
}

void createFloatNumba(void* state, float value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();
    dyncall.addArgF(value);
}

void createDoubleNumba(void* state, double value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();
    dyncall.addArgD(value);
}

void createInt8Numba(void* state, int8_t value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();
    dyncall.addArgI8(value);
}

void createInt16Numba(void* state, int16_t value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();
    dyncall.addArgI16(value);
}

void createInt32Numba(void* state, int32_t value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();
    dyncall.addArgI32(value);
}

void createInt64Numba(void* state, int64_t value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();
    dyncall.addArgI64(value);
}

template<typename T>
T executeNumba(void* state){
    Timer executeNumbaTimer("executeNumba");
    executeNumbaTimer.start();
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    auto dyncall = handler->getDynCall();
    std::string addressVariableName = handler->getFunctionName() + "_address";
    auto addressAsStringPyObject = PyObject_GetAttrString(handler->getPythonModule(), addressVariableName.c_str());
    pythonInterpreterErrorCheck(addressAsStringPyObject, __func__, __LINE__, "Could not get address object.");
    uintptr_t functionAddress = PyLong_AsUnsignedLongLong(addressAsStringPyObject);
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
    executeNumbaTimer.snapshot("execute numba");
    executeNumbaTimer.pause();
    auto path = std::filesystem::current_path().string() + "/dump/executepython.txt";
    std::ofstream outputFile;
    outputFile.open(path, std::ios_base::app);
    outputFile << executeNumbaTimer.getPrintTime() << "\n";
    outputFile.close();
    return result;
}

bool executeToBooleanNumba(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executeNumba<bool>(state);
}

float executeToFloatNumba(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executeNumba<float>(state);
}

double executeToDoubleNumba(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executeNumba<double>(state);
}

int8_t executeToInt8Numba(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executeNumba<int8_t>(state);
}

int16_t executeToInt16Numba(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executeNumba<int16_t>(state);
}

int32_t executeToInt32Numba(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executeNumba<int32_t>(state);
}

int64_t executeToInt64Numba(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler should not be null");
    return executeNumba<int64_t>(state);
}


}// namespace NES::Runtime::Execution::Operators