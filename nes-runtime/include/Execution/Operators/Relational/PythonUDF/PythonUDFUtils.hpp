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

#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <fstream>
#include <Python.h>

#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFUTILS_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFUTILS_HPP_

namespace NES::Runtime::Execution::Operators {

/**
     * @brief Checks for an exception in the python Interpreter and throws a runtime error if one is found.
     *
     * @param pyObject Python Object
     * @param func_name name of the function where the error occurred: should be __func__
     * @param line_number line number where the error occurred: should be __LINE__
     * @param errorMessage error message that is shown
     */
void pythonInterpreterErrorCheck(PyObject* pyObject, const char* func_name, int line_number, std::string errorMessage);

void createBooleanPythonObject(void* state, bool value);
void createStringPythonObject(void* state, TextValue* value);
void createFloatPythonObject(void* state, float value);
void createDoublePythonObject(void* state, double value);
void createIntegerPythonObject(void* state, int32_t value);
void createLongPythonObject(void* state, int64_t value);
void createShortPythonObject(void* state, int16_t value);
void createBytePythonObject(void* state, int8_t value);
void createPythonEnvironment(void* state);
void initPythonTupleSize(void* state, int size);
void setPythonArgumentAtPosition(void* state, int position);
template<typename T>
T transformOutputType(void* outputPtr, int position, int tupleSize);
bool transformBooleanType(void* outputPtr, int position, int tupleSize);
TextValue* transformStringType(void* outputPtr, int position, int tupleSize);
float transformFloatType(void* outputPtr, int position, int tupleSize);
double transformDoubleType(void* outputPtr, int position, int tupleSize);
int32_t transformIntegerType(void* outputPtr, int position, int tupleSize);
int64_t transformLongType(void* outputPtr, int position, int tupleSize);
int16_t transformShortType(void* outputPtr, int position, int tupleSize);
int8_t transformByteType(void* outputPtr, int position, int tupleSize);
void finalizePython(void* state);
void freeObject(void* object);
};// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFUTILS_HPP_
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFUTILS_HPP_
