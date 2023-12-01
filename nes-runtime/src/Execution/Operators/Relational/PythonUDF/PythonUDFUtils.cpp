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

#include <Execution/Operators/Relational/PythonUDF/PythonUDFUtils.hpp>

namespace NES::Runtime::Execution::Operators {

void pythonInterpreterErrorCheck(PyObject* pyObject, const char* func_name, int line_number, std::string errorMessage) {
    if (pyObject == NULL) {
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }
        NES_THROW_RUNTIME_ERROR("[" << func_name << ": line " << line_number << "] " << errorMessage);
    }
}

/**
 * @brief Creates the boolean object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value boolean value
 */
void createBooleanPythonObject(void* state, bool value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    if (value) {
        handler->setPythonVariable(Py_True);
    } else {
        handler->setPythonVariable(Py_False);
    }
}

/**
 * @brief Creates the string object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value TextValue value
 */
void createStringPythonObject(void* state, TextValue* value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->setPythonVariable(PyUnicode_FromString(value->strn_copy().data()));
}

/**
 * @brief Creates the float object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value float value
 */
void createFloatPythonObject(void* state, float value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    // The Python C-API only has PyFloat_FromDouble for all Floating Point Objects
    // Calling this function returns a PyFloatObject
    handler->setPythonVariable(PyFloat_FromDouble(value));
}

/**
 * @brief Creates the double object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value double value
 */
void createDoublePythonObject(void* state, double value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    // The Python C-API only has PyFloat_FromDouble for all Floating Point Objects
    // Calling this function returns a PyFloatObject
    handler->setPythonVariable(PyFloat_FromDouble(value));
}

/**
 * @brief Creates the integer object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value integer value
 */
void createIntegerPythonObject(void* state, int32_t value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    // The Python C-API only has PyLong_FromLong for all Integer Objects
    // Calling this function returns a PyLongObject
    handler->setPythonVariable(PyLong_FromLong(value));
}

/**
 * @brief Creates the long object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value long value
 */
void createLongPythonObject(void* state, int64_t value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    // The Python C-API only has PyLong_FromLong for all Integer Objects
    // Calling this function returns a PyLongObject
    handler->setPythonVariable(PyLong_FromLong(value));
}

/**
 * @brief Creates the short object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value short value
 */
void createShortPythonObject(void* state, int16_t value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    // The Python C-API only has PyLong_FromLong for all Integer Objects
    // Calling this function returns a PyLongObject
    handler->setPythonVariable(PyLong_FromLong(value));
}

/**
 * @brief Creates the byte object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value byte value
 */
void createBytePythonObject(void* state, int8_t value) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    // The Python C-API only has PyLong_FromLong for all Integer Objects
    // Calling this function returns a PyLongObject
    handler->setPythonVariable(PyLong_FromLong(value));
}

/**
 * @brief Initalizes the python interpreter
 * @param state PythonUDFOperatorHandler
 */
void createPythonEnvironment(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->initPython();
}

/**
 * @brief Creates a python tuple with a specific size and set it as the argument
 * @param state PythonUDFOperatorHandler
 * @param size of tuple
 */
void initPythonTupleSize(void* state, int size) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    //PyObject* pythonArguments = handler->getPythonArguments();
    PyObject* pythonArguments = PyTuple_New(size);
    handler->setPythonArguments(pythonArguments);
}

/**
 * @brief Adds the value that we set in the create functions into the python tuple (the argument)
 * @param state PythonUDFOperatorHandler
 * @param position position inside tuple
 */
void setPythonArgumentAtPosition(void* state, int position) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    PyObject* pythonArguments = handler->getPythonArguments();
    PyObject* pythonValue = handler->getPythonVariable();
    PyTuple_SetItem(pythonArguments, position, pythonValue);
    handler->setPythonArguments(pythonArguments);
}
/**
 * @brief Transforms python object output into c++ data types
 * @tparam T
 * @param state
 * @param outputPtr
 * @param position
 * @param tupleSize
 * @return
 */
template<typename T>
T transformOutputType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    auto output = static_cast<PyObject*>(outputPtr);
    if (tupleSize > 1) {
        output = PyTuple_GetItem(output, position);
    }

    // value in the specific data type that we want to transform the PyObject into
    T value;
    if constexpr (std::is_same<T, bool>::value) {
        if (PyObject_IsTrue(output)) {
            value = true;
        } else {
            value = false;
        }
    } else if constexpr (std::is_same<T, float>::value) {
        // The Python C-API only has PyFloat_AsDouble for all Floating Point Objects
        // Calling this function returns a double
        value = PyFloat_AsDouble(output);
    } else if constexpr (std::is_same<T, double>::value) {
        value = PyFloat_AsDouble(output);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        // The Python C-API only has PyLong_AsLong for all Integer Objects
        // Calling this function returns a long because all integers are implemented as long in Python
        value = PyLong_AsLong(output);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        value = PyLong_AsLong(output);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        value = PyLong_AsLong(output);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        value = PyLong_AsLong(output);
    } else if constexpr (std::is_same<T, TextValue*>::value) {
        auto string = PyUnicode_AsUTF8(output);
        value = TextValue::create(string);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    return value;
}

/**
 * @brief Transforms the PyObject into a boolean
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++ data type
 */
bool transformBooleanType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    return transformOutputType<bool>(outputPtr, position, tupleSize);
}

/**
 * @brief Transforms the PyObject into a TextValue
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
TextValue* transformStringType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    return transformOutputType<TextValue*>(outputPtr, position, tupleSize);
}

/**
 * @brief Transforms the PyObject into a float
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
float transformFloatType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    return transformOutputType<float>(outputPtr, position, tupleSize);
}

/**
 * @brief Transforms the PyObject into a boolean
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
double transformDoubleType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    return transformOutputType<double>(outputPtr, position, tupleSize);
}

/**
 * @brief Transforms the PyObject into an integer
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
int32_t transformIntegerType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    return transformOutputType<int32_t>(outputPtr, position, tupleSize);
}

/**
 * @brief Transforms the PyObject into a long
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
int64_t transformLongType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    return transformOutputType<int64_t>(outputPtr, position, tupleSize);
}

/**
 * @brief Transforms the PyObject into a short
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
int16_t transformShortType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    return transformOutputType<int16_t>(outputPtr, position, tupleSize);
}

/**
 * @brief Transforms the PyObject into a byte
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
int8_t transformByteType(void* outputPtr, int position, int tupleSize) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "OutputPtr should not be null");
    return transformOutputType<int8_t>(outputPtr, position, tupleSize);
}

/**
 * @brief Undo initialization of the python interpreter
 * @param state
 */
void finalizePython(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "handler should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->finalize();
}


}// namespace NES::Runtime::Execution::Operators