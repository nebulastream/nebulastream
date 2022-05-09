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

#ifdef PYTHON_UDF_ENABLED
#include <QueryCompiler/Operators/PhysicalOperators/PythonUdfExecutablePipelineStage.hpp>
#include "Runtime/MemoryLayout/DynamicTupleBuffer.hpp"
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

PythonUdfExecutablePipelineStage::PythonUdfExecutablePipelineStage(const SchemaPtr& inputSchema, const SchemaPtr& outputSchema) {
    this->inputSchema = inputSchema;
    this->outputSchema = outputSchema;
}

/**
 * t = {x: int, y: float, z: bool}
 *
 * Select sum(pyFunction(t.x)) From trips t -> Query::from("trips").apply(Sum(udf(pyFunction(t.x))))
 *                                          ->                     .apply(Sum(udf(pyFunction, Attribute("x"))))
 *
 * Select z From trips t Where pyFunction(t.x, t.y) > 10
 *                                          -> Query::from("trips").filter(udf(t.x, t.y) > 10)
 *                                          -> Query::from("trips").filter(udf(pyFunction, Attribute("x"), Attribute("y")) > 10)
 *
 * First goal: Query::from("trips").map(Attribute("x") = udf(...))
 *
 * def pyFunction(x, y):
 *      return x * y
 *
 *
 * def pyFunction2(x: int, y: int, z: char, b: bool):
 *      if b:
 *          ...
 */

ExecutionResult PythonUdfExecutablePipelineStage::execute(TupleBuffer& inputTupleBuffer,
                                                          Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                                          Runtime::WorkerContext& workerContext) {
    auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(this->inputSchema, inputTupleBuffer.getBufferSize());
    auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, inputTupleBuffer);

    PyObject *pyName, *pyModule, *pyFunc, *pyArgs, *pyValue, *pyUdfDirectory;

    //TODO move init process out of execute
    Py_Initialize();
    pyUdfDirectory = PyUnicode_FromString(this->udfDirectory);
    PyList_Insert(PySys_GetObject(this->pathName), 0, pyUdfDirectory);
    pyName = PyUnicode_FromString(this->udfFilename);
    pyModule = PyImport_Import(pyName);
    // When a PyObject is no longer needed, we need to decrease the reference counter
    // so that the Python garbage collector knows when to free an object.
    // Initializing a PyObject sets the reference counter to 1, so no Py_INCREF() is needed.
    Py_DECREF(pyName);
    Py_DECREF(pyUdfDirectory);
    if (pyModule != nullptr) {
        pyFunc = PyObject_GetAttrString(pyModule, this->pythonFunctionName);
        if (pyFunc && PyCallable_Check(pyFunc)) {
            // Iterate over tuples
            for (uint64_t t = 0; t < dynamicTupleBuffer.getNumberOfTuples(); t++) {
                // Iterate over elements in tuple
                for (uint64_t i = 0; i < this->inputSchema->getSize(); i++) {
                    pyArgs = PyTuple_New(1);
                    pyValue = PyLong_FromLong(dynamicTupleBuffer[t][i].read<int64_t>());
                    if (!pyValue) {
                        Py_Finalize();// Frees all memory allocated
                        NES_ERROR("Unable to convert value");
                        return ExecutionResult::Error;
                    }
                    PyTuple_SetItem(pyArgs, 0, pyValue);
                    // The python function call happens here
                    pyValue = PyObject_CallObject(pyFunc, pyArgs);
                    Py_DECREF(pyArgs);
                    if (pyValue != nullptr) {
                        dynamicTupleBuffer[t][i].write(PyLong_AsLong(pyValue));
                        Py_DECREF(pyValue);
                    } else {
                        Py_Finalize();
                        NES_ERROR("Function call failed");
                        return ExecutionResult::Error;
                    }
                }
            }
        } else {
            if (PyErr_Occurred()) {
                PyErr_Print();
            }
            Py_Finalize();
            NES_ERROR("Cannot find function " << this->pythonFunctionName);
            return ExecutionResult::Error;
        }
    } else {
        PyErr_Print();
        Py_Finalize();
        NES_ERROR("Failed to load " << this->udfFilename);
        return ExecutionResult::Error;
    }
    if (Py_FinalizeEx() < 0) {
        return ExecutionResult::Error;
    }
    pipelineExecutionContext.emitBuffer(inputTupleBuffer, workerContext);
    return ExecutionResult::Ok;
}

PythonUdfExecutablePipelineStage::~PythonUdfExecutablePipelineStage(){
    NES_DEBUG("~PythonUdfExecutablePipelineStage()");
};

}//namespace NES
#endif