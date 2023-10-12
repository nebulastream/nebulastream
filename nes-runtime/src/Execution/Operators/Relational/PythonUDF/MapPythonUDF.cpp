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
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFNumbaUtils.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFUtils.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Python.h>
#include <Util/Timer.hpp>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Executes the python udf using the default python compiler
 * @param state is the python udf operator handler
 * @return the result of the python udf
 */
void* executeMapUdf(void* state) {
    Timer executeCPython("executeCPython");
    executeCPython.start();
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    // get module and python arguments for the udf
    // we need the module bc inside this module is the python udf
    auto* pythonArguments = handler->getPythonArguments();
    pythonInterpreterErrorCheck(pythonArguments, __func__, __LINE__, "No arguments for the python udf can be found.");

    PyObject* module = handler->getPythonModule();
    pythonInterpreterErrorCheck(pythonArguments, __func__, __LINE__, "Could not get the python module.");

    // we get the python function
    auto pythonFunction = PyObject_GetAttrString(module, handler->getFunctionName().c_str());
    pythonInterpreterErrorCheck(pythonArguments,
                                __func__,
                                __LINE__,
                                "Could not find the python udf " + handler->getFunctionName() + " inside the module");

    if (!PyCallable_Check(pythonFunction)) {
        //  PyCallable_Check returns 1 if the object is callable (= is a function) and 0 otherwise.
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }
        NES_THROW_RUNTIME_ERROR("Function not callable");
    }

    // execute python udf
    auto result = PyObject_CallObject(pythonFunction, pythonArguments);
    pythonInterpreterErrorCheck(pythonArguments,
                                __func__,
                                __LINE__,
                                "Something went wrong. Result of the Python UDF is NULL");
    executeCPython.snapshot("Executed CPython");
    executeCPython.pause();
    auto path = std::filesystem::current_path().string() + "/dump/executepython.txt";
    std::ofstream outputFile;
    outputFile.open(path, std::ios_base::app);
    outputFile << executeCPython.getPrintTime() << "\n";
    outputFile.close();
    return result;
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
    PyObject* pythonArguments = handler->getPythonArguments();
    pythonArguments = PyTuple_New(size);
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

/**
 * @brief checks whether we have to use numba
 * @param state
 * @return boolean true if we have to use numba
 */
bool useNumba(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    bool result = false;
    if (handler->getPythonCompiler() == "numba") {
        result = true;
    }
    return result;
}

/**
 * @brief Operator execution function
 * @param ctx operator context
 * @param record input record
 */
void MapPythonUDF::execute(ExecutionContext& ctx, Record& record) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // FunctionCall("createPythonEnvironment", createPythonEnvironment, handler);
    //auto numbaActivated = FunctionCall("useNumba", useNumba, handler);
    if (pythonCompiler == "numba") {
        // add Parameters
        for (int i = 0; i < (int) inputSchema->fields.size(); i++) {
            auto field = inputSchema->fields[i];
            auto fieldName = field->getName();
            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                FunctionCall("createBooleanNumba", createBooleanNumba, handler, record.read(fieldName).as<Boolean>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                FunctionCall("createFloatNumba", createFloatNumba, handler, record.read(fieldName).as<Float>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                FunctionCall("createDoubleNumba", createDoubleNumba, handler, record.read(fieldName).as<Double>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                FunctionCall("createInt8Numba", createInt8Numba, handler, record.read(fieldName).as<Int8>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                FunctionCall("createInt16Numba", createInt16Numba, handler, record.read(fieldName).as<Int16>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                FunctionCall("createInt32Numba", createInt32Numba, handler, record.read(fieldName).as<Int32>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                FunctionCall("createInt64Numba", createInt64Numba, handler, record.read(fieldName).as<Int64>());
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
        }
        // execute function through function pointer and write result into record
        record = Record();
        if (outputSchema->fields.size() == 1) {
            auto field = outputSchema->fields[0];
            auto fieldName = field->getName();
            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                Value<> val = FunctionCall("executeToBoolean", executeToBoolean, handler);
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                Value<> val = FunctionCall("executeToFloat", executeToFloat, handler);
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                Value<> val = FunctionCall("executeToDouble", executeToDouble, handler);
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                Value<> val = FunctionCall("executeToInt8", executeToInt8, handler);
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                Value<> val = FunctionCall("executeToInt16", executeToInt16, handler);
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                Value<> val = FunctionCall("executeToInt32", executeToInt32, handler);
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                Value<> val = FunctionCall("executeToInt64", executeToInt64, handler);
                record.write(fieldName, val);
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
        } else {
            NES_THROW_RUNTIME_ERROR("Cannot run this function with more than one output variable");
        }
    } else {
        FunctionCall("initPythonTupleSize", initPythonTupleSize, handler, Value<Int32>((int) inputSchema->fields.size()));

        // check for data type
        for (int i = 0; i < (int) inputSchema->fields.size(); i++) {
            auto field = inputSchema->fields[i];
            auto fieldName = field->getName();

            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                FunctionCall("createBooleanPythonObject", createBooleanPythonObject, handler, record.read(fieldName).as<Boolean>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                FunctionCall("createFloatPythonObject", createFloatPythonObject, handler, record.read(fieldName).as<Float>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                FunctionCall("createDoublePythonObject", createDoublePythonObject, handler, record.read(fieldName).as<Double>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                FunctionCall("createIntegerPythonObject",
                             createIntegerPythonObject,
                             handler,
                             record.read(fieldName).as<Int32>());// Integer
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                FunctionCall("createLongPythonObject", createLongPythonObject, handler, record.read(fieldName).as<Int64>());// Long
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                FunctionCall("createShortPythonObject", createShortPythonObject, handler, record.read(fieldName).as<Int16>());// Short
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                FunctionCall("createBytePythonObject", createBytePythonObject, handler, record.read(fieldName).as<Int8>());// Byte
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
            FunctionCall("setPythonArgumentAtPosition", setPythonArgumentAtPosition, handler, Value<Int32>(i));
        }
        auto outputPtr = FunctionCall<>("executeMapUdf", executeMapUdf, handler);

        record = Record();

        int outputSize = (int) outputSchema->fields.size();
        for (int i = 0; i < outputSize; i++) {
            auto field = outputSchema->fields[i];
            auto fieldName = field->getName();

            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                Value<> val =
                    FunctionCall("transformBooleanType", transformBooleanType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                Value<> val =
                    FunctionCall("transformFloatType", transformFloatType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                Value<> val =
                    FunctionCall("transformDoubleType", transformDoubleType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                Value<> val =
                    FunctionCall("transformIntegerType", transformIntegerType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
                record.write(fieldName, val);// Integer
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                Value<> val =
                    FunctionCall("transformLongType", transformLongType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
                record.write(fieldName, val);// Long
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                Value<> val =
                    FunctionCall("transformShortType", transformShortType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
                record.write(fieldName, val);// Short
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                Value<> val =
                    FunctionCall("transformByteType", transformByteType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
                record.write(fieldName, val);// Byte
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
        }
    }
    // Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

/**
 * @brief Terminate operator
 * @param ctx execution context
 */
void MapPythonUDF::terminate(ExecutionContext& ctx) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    //FunctionCall<>("finalizePython", finalizePython, handler);
}
}// namespace NES::Runtime::Execution::Operators
#endif//NAUTILUS_PYTHON_UDF_ENABLED
