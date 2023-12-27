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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/PythonUDF/FlatMapPythonUDF.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFUtils.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <filesystem>
#include <fstream>
#include <utility>
#if not(defined(__APPLE__))
#include <experimental/source_location>
#endif

namespace NES::Runtime::Execution::Operators {

FlatMapPythonUDF::FlatMapPythonUDF(uint64_t operatorHandlerIndex, SchemaPtr inputSchema, SchemaPtr outputSchema, std::string pythonCompiler)
    : operatorHandlerIndex(operatorHandlerIndex), operatorInputSchema(inputSchema), operatorOutputSchema(outputSchema), pythonCompiler(pythonCompiler){};

Record FlatMapPythonUDF::extractRecordFromObject(const Value<MemRef>& outputPtr) const {
    // outputPtr is only available for CPython, Cython and Nuitka
    // Create new record for result
    auto record = Record();
    int outputSize = (int) operatorOutputSchema->fields.size();
    for (int i = 0; i < outputSize; i++) {
        auto field = operatorOutputSchema->fields[i];
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

    return record;
}

void FlatMapPythonUDF::createInputPojo(Record& record, Value<MemRef>& handler) const {
    FunctionCall("initPythonTupleSize", initPythonTupleSize, handler, Value<Int32>((int) operatorInputSchema->fields.size()));

    // check for data type
    for (int i = 0; i < (int) operatorInputSchema->fields.size(); i++) {
        auto field = operatorInputSchema->fields[i];
        auto fieldName = field->getName();

        if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
            FunctionCall("createBooleanPythonObject",
                         createBooleanPythonObject,
                         handler,
                         record.read(fieldName).as<Boolean>());
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
            FunctionCall("createLongPythonObject",
                         createLongPythonObject,
                         handler,
                         record.read(fieldName).as<Int64>());// Long
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
            FunctionCall("createShortPythonObject",
                         createShortPythonObject,
                         handler,
                         record.read(fieldName).as<Int16>());// Short
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
            FunctionCall("createBytePythonObject", createBytePythonObject, handler, record.read(fieldName).as<Int8>());// Byte
        } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
            FunctionCall<>("createStringPythonObject",
                           createStringPythonObject,
                           handler,
                           record.read(fieldName).as<Text>()->getReference());// String
        } else {
            NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
        }
        FunctionCall("setPythonArgumentAtPosition", setPythonArgumentAtPosition, handler, Value<Int32>(i));
    }
}

void* executeFlatMapUDF(void* state) {
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
    pythonInterpreterErrorCheck(result,
                                __func__,
                                __LINE__,
                                "Something went wrong. Result of the Python UDF is NULL");

    pythonUDFExecutionTimeTimer.snapshot("executePythonUDF");
    pythonUDFExecutionTimeTimer.pause();
    handler->addSumExecution(pythonUDFExecutionTimeTimer.getPrintTime());
    return result;
}

void* next(void* outputPtr, int index) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "op handler context should not be null");
    auto output = static_cast<PyObject*>(outputPtr);
    Py_ssize_t pyIndex = (Py_ssize_t) index;
    auto element = PyList_GetItem(output, pyIndex);
    return element;
}

uint64_t getLengthOfOutputList(void*, void* outputPtr) {
    NES_ASSERT2_FMT(outputPtr != nullptr, "op handler context should not be null");
    auto output = static_cast<PyObject*>(outputPtr);
    if (PyList_Check(output)) {
        auto pyListSize = PyList_Size(output);
        return pyListSize;
    } else {
        return 0;
    }
}

void FlatMapPythonUDF::execute(ExecutionContext& ctx, Record& record) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // create variables for input pojo ptr java input class
    createInputPojo(record, handler);

    // Call udf and get the result value.
    // For flatmap we assume that they return a collection of values.
    auto outputPojoPtr = FunctionCall<>("executeFlatMapUDF", executeFlatMapUDF, handler);
    auto getLengthOfOutputPtr = FunctionCall("getLengthOfOutputList", getLengthOfOutputList, handler, outputPojoPtr);
    for (Value<UInt64> i = 0_u64; i < getLengthOfOutputPtr; i = i + 1_u64) {
        auto element = FunctionCall("next", next, outputPojoPtr, i);
        auto resultRecord = extractRecordFromObject(element);
        // Trigger execution of next operator
        child->execute(ctx, resultRecord);
    }
    FunctionCall<>("freeObject", freeObject, outputPojoPtr);

}

}// namespace NES::Runtime::Execution::Operators