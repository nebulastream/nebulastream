#ifdef MIS_PYTHON_UDF_ENABLED
#include <Execution/Operators/ExecutionContext.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp>
#include <Python.h>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <cstring>
#include <fstream>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief executes the python udf
 * @param state is the python udf operator handler
 * @return the result of the python udf
 */
void* executeMapUdf(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);

    // get module and python arguments for the udf
    // we need the module bc inside this module is the python udf
    auto *pythonArguments = handler->getPythonArguments();
    if (pythonArguments == NULL) {
            if (PyErr_Occurred()) {
                PyErr_Print();
                PyErr_Clear();
            }
            NES_THROW_RUNTIME_ERROR("No arguments for the python udf can be found.");
        }

    PyObject *module = handler->getPythonModule();

    if(module == NULL) {
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }
        NES_THROW_RUNTIME_ERROR("Could not get the python module.");
    }

    // we get the python function
    auto pythonFunction = PyObject_GetAttrString(module, handler->getFunctionName().c_str());
    if (pythonFunction == NULL) {
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }
        NES_THROW_RUNTIME_ERROR("Could not find the python udf " << handler->getFunctionName() << " inside the module");
    }

    if(!PyCallable_Check(pythonFunction)){
        //  PyCallable_Check returns 1 if the object is callable (= is a function) and 0 otherwise.
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }
        NES_THROW_RUNTIME_ERROR("Function not callable");
    }

    // execute python udf
    auto result = PyObject_CallObject(pythonFunction, pythonArguments);
    if (result == NULL) {
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }
        NES_THROW_RUNTIME_ERROR("Something went wrong. Result of the Python UDF is NULL");
    }
    return result;
}

/**
 * @brief creates the boolean object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value boolean value
 */
void createBooleanObject(void* state, bool value) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    if(value) {
        handler->setPythonVariable(Py_True);
    } else {
        handler->setPythonVariable(Py_False);
    }
}

/**
 * @brief creates the float object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value float value
 */
void createFloatObject(void* state, float value) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->setPythonVariable(PyFloat_FromDouble(value));
}

/**
 * @brief creates the double object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value double value
 */
void createDoubleObject(void* state, double value) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->setPythonVariable(PyFloat_FromDouble(value));
}

/**
 * @brief creates the integer object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value integer value
 */
void createIntegerObject(void* state, int32_t value) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->setPythonVariable(PyLong_FromLong(value));
}

/**
 * @brief creates the long object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value long value
 */
void createLongObject(void* state, int64_t value) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->setPythonVariable(PyLong_FromLong(value));
}

/**
 * @brief creates the short object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value short value
 */
void createShortObject(void* state, int16_t value) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->setPythonVariable(PyLong_FromLong(value));
}

/**
 * @brief creates the byte object for the python udf argument
 * @param state PythonUDFOperatorHandler
 * @param value byte value
 */
void createByteObject(void* state, int8_t value) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->setPythonVariable(PyLong_FromLong(value));
}

/**
 * @brief initalizes the python interpreter
 * @param state PythonUDFOperatorHandler
 */
void createPythonEnvironment(void* state) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->initPython();
}

/**
 * @brief creates a python tuple with a specific size and set it as the argument
 * @param state PythonUDFOperatorHandler
 * @param size of tuple
 */
void initPythonTupleSize(void* state, int size) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    PyObject *pythonArguments = handler->getPythonArguments();
    pythonArguments = PyTuple_New(size);
    handler->setPythonArguments(pythonArguments);
}

/**
 * @brief adds the value that we set in the create functions into the python tuple (the argument)
 * @param state PythonUDFOperatorHandler
 * @param position position inside tuple
 */
void setPythonTupleAtPosition(void* state, int position) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    PyObject *pythonArguments = handler->getPythonArguments();
    PyObject *pythonValue = handler->getPythonVariable();
    PyTuple_SetItem(pythonArguments, position, pythonValue);
    handler->setPythonArguments(pythonArguments);
}
/**
 * @brief transforms python object output into c++ data types
 * @tparam T
 * @param state
 * @param outputPtr
 * @param position
 * @param tupleSize
 * @return
 */
template<typename T>
T transformOutputType(void* outputPtr, int position, int tupleSize) {
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
    } else if  constexpr (std::is_same<T, float>::value){
        value = PyFloat_AsDouble(output);
    } else if constexpr (std::is_same<T, double>::value) {
        value = PyFloat_AsDouble(output);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        value = PyLong_AsLong(output);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        value = PyLong_AsLong(output);
    }  else if constexpr (std::is_same<T, int16_t>::value) {
        value = PyLong_AsLong(output);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        value = PyLong_AsLong(output);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    return value;
}

/**
 * @brief transforms the PyObject into a boolean
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
bool transformBooleanType(void* outputPtr, int position, int tupleSize) {
    return transformOutputType<bool>(outputPtr, position, tupleSize);
}

/**
 * @brief transforms the PyObject into a float
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
float transformFloatType(void* outputPtr, int position, int tupleSize) {
    return transformOutputType<float>(outputPtr, position, tupleSize);
}

/**
 * @brief transforms the PyObject into a boolean
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
double transformDoubleType(void* outputPtr, int position, int tupleSize) {
    return transformOutputType<double>(outputPtr, position, tupleSize);
}

/**
 * @brief transforms the PyObject into an integer
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
int32_t transformIntegerType(void* outputPtr, int position, int tupleSize) {
    return transformOutputType<int32_t>(outputPtr, position, tupleSize);
}

/**
 * @brief transforms the PyObject into a long
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
int64_t transformLongType(void* outputPtr, int position, int tupleSize) {
    return transformOutputType<int64_t>(outputPtr, position, tupleSize);
}

/**
 * @brief transforms the PyObject into a short
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
int16_t transformShortType(void* outputPtr, int position, int tupleSize) {
    return transformOutputType<int16_t>(outputPtr, position, tupleSize);
}

/**
 * @brief transforms the PyObject into a byte
 * @param outputPtr pyObject as a python tuple
 * @param position position in the pyObject tuple
 * @param tupleSize size of the tuple
 * @return transformed output as a c++  data type
 */
int8_t transformByteType(void* outputPtr, int position, int tupleSize) {
    return transformOutputType<int8_t>(outputPtr, position, tupleSize);
}

/**
 * @brief undo initialization of the python interpreter
 * @param state
 */
void finalizePython(void* state) {
    auto handler = static_cast<PythonUDFOperatorHandler*>(state);
    handler->finalize();
}

/**
 * Operator execution function
 * @param ctx operator context
 * @param record input record
 */
void MapPythonUDF::execute(ExecutionContext& ctx, Record& record) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    FunctionCall("createPythonEnvironment", createPythonEnvironment, handler);

    FunctionCall("initPythonTupleSize", initPythonTupleSize, handler, Value<Int32>((int) inputSchema->fields.size()));

    // check for data type
    for (int i = 0; i < (int) inputSchema->fields.size(); i++) {
        auto field = inputSchema->fields[i];
        auto fieldName = field->getName();

        if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
            FunctionCall("createBooleanObject", createBooleanObject, handler, record.read(fieldName).as<Boolean>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
            FunctionCall("createFloatObject", createFloatObject, handler, record.read(fieldName).as<Float>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
            FunctionCall("createDoubleObject", createDoubleObject, handler, record.read(fieldName).as<Double>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
            FunctionCall("createIntegerObject", createIntegerObject, handler, record.read(fieldName).as<Int32>()); // Integer
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
            FunctionCall("createLongObject", createLongObject, handler, record.read(fieldName).as<Int64>()); // Long
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
            FunctionCall("createShortObject", createShortObject, handler, record.read(fieldName).as<Int16>()); // Short
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
            FunctionCall("createByteObject", createByteObject, handler, record.read(fieldName).as<Int8>());// Byte
        } else {
            NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
        }
        FunctionCall("setPythonTupleAtPosition", setPythonTupleAtPosition, handler, Value<Int32>(i));
    }
    auto outputPtr = FunctionCall<>("executeMapUdf", executeMapUdf, handler);

    record = Record();

    int outputSize = (int) outputSchema->fields.size();
    for (int i = 0; i < outputSize; i++) {
        auto field = outputSchema->fields[i];
        auto fieldName = field->getName();

        if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
            Value<> val = FunctionCall("transformBooleanType", transformBooleanType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
            Value<> val = FunctionCall("transformFloatType", transformFloatType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
            Value<> val = FunctionCall("transformDoubleType", transformDoubleType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
            Value<> val = FunctionCall("transformIntegerType", transformIntegerType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
            record.write(fieldName, val); // Integer
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
            Value<> val = FunctionCall("transformLongType", transformLongType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
            record.write(fieldName, val); // Long
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
            Value<> val = FunctionCall("transformShortType", transformShortType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
            record.write(fieldName, val); // Short
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
            Value<> val = FunctionCall("transformByteType", transformByteType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
            record.write(fieldName, val);// Byte
        } else {
            NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
        }
        FunctionCall("setPythonTupleAtPosition", setPythonTupleAtPosition, handler, Value<Int32>(i));
    }
    // Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

/**
 * @brief terminate operator
 * @param ctx execution context
 */
void MapPythonUDF::terminate(ExecutionContext& ctx) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    FunctionCall<>("finalizePython", finalizePython, handler);
}
}
#endif
