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
#ifdef MIS_PYTHON_UDF_ENABLED
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPPYTHONUDFOPERATORHANDLER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPPYTHONUDFOPERATORHANDLER_HPP_

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <mutex>
#include <utility>
#include <Python.h>

namespace NES::Runtime::Execution::Operators {

/**
* @brief This handler stores states of a MapPythonUDF operator during its execution.
*/
class PythonUDFOperatorHandler : public OperatorHandler {
  public:
    explicit PythonUDFOperatorHandler(const std::string& function,
                                      const std::string& functionName,
                                    SchemaPtr inputSchema,
                                    SchemaPtr outputSchema)
        : function(function), functionName(functionName), inputSchema(inputSchema),
          outputSchema(outputSchema) {}

    /**
     * @brief This method returns the udf as a string
     * @return std::string python udf
     */
    const std::string& getFunction() const { return this->function; }

    /**
     * @brief This method returns the udf as a string
     * @return std::string python udf
     */
    const std::string& getFunctionName() const { return this->functionName; }


    /**
     * @brief This method returns the input schema of the python udf
     * @return SchemaPtr input schema
     */
    const SchemaPtr& getInputSchema() const { return this->inputSchema; }

    /**
     * @brief This method returns the output schema of the python udf
     * @return SchemaPtr output schema
     */
    const SchemaPtr& getOutputSchema() const { return this->outputSchema; }

    /**
     * @brief initializes the python udf in a module
     */
    void initPython() {
        this->moduleName = this->functionName + "Module";
        // initialize python interpreter
        Py_Initialize();
        PyObject* pythonCode = Py_CompileString(this->function.c_str(), "", Py_file_input);
        if (pythonCode == NULL) {
            if (PyErr_Occurred()) {
                PyErr_Print();
                PyErr_Clear();
                NES_THROW_RUNTIME_ERROR("Could not compile String.");
            }
        }
        // add python code into our module
        this->pythonModule = PyImport_ExecCodeModule(this->moduleName.c_str(), pythonCode);
        if (this->pythonModule == NULL) {
            if (PyErr_Occurred()) {
                PyErr_Print();
                PyErr_Clear();
            }
            NES_THROW_RUNTIME_ERROR("Cannot add function " << this->functionName << " to module " << this->moduleName);
        }
        // set function to this newly created function
       /*this->pythonFunction = PyObject_GetAttrString(this->pythonModule, this->functionName.c_str());

        if (this->pythonFunction == NULL) {
            if (PyErr_Occurred()) {
                PyErr_Print();
                PyErr_Clear();
            }
            NES_THROW_RUNTIME_ERROR("Cannot find function " << this->functionName << " inside module " << this->moduleName);
        }*/
    }

    /**
     * @brief undo all initialization of the python interpreter
     */
    void finalize() {
        Py_DecRef(this->pythonVariable);
        Py_DecRef(this->pythonModule);
        Py_DecRef(this->pythonArguments);

        if (Py_IsInitialized()) {
            if (Py_FinalizeEx() == -1) {
                PyErr_Print();
                PyErr_Clear();
                NES_THROW_RUNTIME_ERROR("Something went wrong with finalizing Python");
            }
        }

    }

    /**
     * @brief getter function for the module name
     * @return module name as string
     */
    std::string getModuleName() const { return this->moduleName; }

    /**
     * @brief getter function for the arguments that go into the python udf function
     * @return returns python arguments as a PyObject
     */
    PyObject* getPythonArguments() const { return this->pythonArguments; }

    /**
     * @brief sets the arguments that go into the python udf function
     * @param pythonArguments arguments for the udf function as a PyObject
     */
    void setPythonArguments(PyObject* pythonArguments) {this->pythonArguments = pythonArguments; }

    /**
     * @brief getter function for the arguments that go into the python udf function
     * @return returns python arguments as a PyObject
     */
    PyObject* getPythonVariable() const { return this->pythonVariable; }

    /**
     * @brief sets the arguments that go into the python udf function
     * @param pythonArguments arguments for the udf function as a PyObject
     */
    void setPythonVariable(PyObject* pythonVariable) {this->pythonVariable = pythonVariable; }
    /**
     * @brief getter function for the python function
     * @return the python function as a PyObject
     */
    PyObject* getPythonFunction() const { return this->pythonFunction; }

    /**
     * @brief getter function for the python module
     * @return python module as a PyObject
     */
    PyObject* getPythonModule() const { return this->pythonModule; }


    void start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) override {}
    void stop(QueryTerminationType, PipelineExecutionContextPtr) override {}

  private:
    const std::string function;
    const std::string functionName;
    const SchemaPtr inputSchema;
    const SchemaPtr outputSchema;
    std::string moduleName;
    PyObject* pythonArguments; // arguments of python user defined function
    PyObject* pythonFunction; // python function object
    PyObject* pythonModule; // python module object
    PyObject* pythonVariable; // temp python variable for setting arguments
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPPYTHONUDFOPERATORHANDLER_HPP_
#endif