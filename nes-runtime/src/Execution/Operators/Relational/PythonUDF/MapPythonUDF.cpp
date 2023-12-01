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
#include <Execution/Operators/Relational/PythonUDF/PythonUDFPyPyUtils.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFUtils.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
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
    Timer pythonUDFExecutionTimeTimer("PythonUDFExecution");
    pythonUDFExecutionTimeTimer.start();
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

    pythonUDFExecutionTimeTimer.snapshot("executePythonUDF");
    pythonUDFExecutionTimeTimer.pause();
    handler->addSumExecution(pythonUDFExecutionTimeTimer.getPrintTime());
    return result;
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
    if (pythonCompiler == "numba" || pythonCompiler == "pypy") {
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
        if (pythonCompiler == "numba") {
            if (outputSchema->fields.size() == 1) {
                auto field = outputSchema->fields[0];
                auto fieldName = field->getName();
                if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                    Value<> val = FunctionCall("executeToBooleanNumba", executeToBooleanNumba, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                    Value<> val = FunctionCall("executeToFloatNumba", executeToFloatNumba, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                    Value<> val = FunctionCall("executeToDoubleNumba", executeToDoubleNumba, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                    Value<> val = FunctionCall("executeToInt8Numba", executeToInt8Numba, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                    Value<> val = FunctionCall("executeToInt16Numba", executeToInt16Numba, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                    Value<> val = FunctionCall("executeToInt32Numba", executeToInt32Numba, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                    Value<> val = FunctionCall("executeToInt64Numba", executeToInt64Numba, handler);
                    record.write(fieldName, val);
                } else {
                    NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
                }
            } else {
                NES_THROW_RUNTIME_ERROR("Cannot run this function with more than one output variable");
            }
        } else if (pythonCompiler == "pypy") {
            if (outputSchema->fields.size() == 1) {
                auto field = outputSchema->fields[0];
                auto fieldName = field->getName();
                if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                    Value<> val = FunctionCall("executeToBooleanPyPy", executeToBooleanPyPy, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                    Value<> val = FunctionCall("executeToFloatPyPy", executeToFloatPyPy, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                    Value<> val = FunctionCall("executeToDoublePyPy", executeToDoublePyPy, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                    Value<> val = FunctionCall("executeToInt8PyPy", executeToInt8PyPy, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                    Value<> val = FunctionCall("executeToInt16PyPy", executeToInt16PyPy, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                    Value<> val = FunctionCall("executeToInt32PyPy", executeToInt32PyPy, handler);
                    record.write(fieldName, val);
                } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                    Value<> val = FunctionCall("executeToInt64PyPy", executeToInt64PyPy, handler);
                    record.write(fieldName, val);
                } else {
                    NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
                }
            } else {
                NES_THROW_RUNTIME_ERROR("Cannot run this function with more than one output variable");
            }
        } else {
            NES_THROW_RUNTIME_ERROR("Something went wrong... Cannot execute this function.");
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
            } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
                FunctionCall<>("createStringPythonObject", createStringPythonObject, handler, record.read(fieldName).as<Text>()->getReference()); // String
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
            FunctionCall("setPythonArgumentAtPosition", setPythonArgumentAtPosition, handler, Value<Int32>(i));
        }
        auto outputPtr = FunctionCall<>("executeMapUdf", executeMapUdf, handler);

        record = Record();

        // extract tuple and one single output
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
            } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
                Value<> val = FunctionCall<>("transformStringType", transformStringType, outputPtr, Value<Int32>(i), Value<Int32>(outputSize));
                record.write(fieldName, val);// String
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
