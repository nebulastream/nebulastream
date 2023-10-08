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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFOPERATORHANDLER_HPP_

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Python.h>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <map>
#include <mutex>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
* @brief This handler stores states of a MapPythonUDF operator during its execution.
*/
class PythonUDFOperatorHandler : public OperatorHandler {
  public:
    explicit PythonUDFOperatorHandler(const std::string& function,
                                      const std::string& functionName,
                                      const std::map<std::string, std::string> modulesToImport,
                                      const std::string& pythonCompiler,
                                      SchemaPtr inputSchema,
                                      SchemaPtr outputSchema)
        : function(function), functionName(functionName), modulesToImport(modulesToImport), pythonCompiler(pythonCompiler), inputSchema(inputSchema), outputSchema(outputSchema) {}

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
     * @brief Getter function for the module name
     * @return module name as string
     */
    std::string getModuleName() const { return this->moduleName; }

    /**
     * @brief Getter function for the python compiler
     * @return name of the compiler as string
     */
    std::string getPythonCompiler() const { return this->pythonCompiler; }

    /**
     * @brief Getter function for the arguments that go into the python udf function
     * @return returns python arguments as a PyObject
     */
    PyObject* getPythonArguments() const { return this->pythonArguments; }

    /**
     * @brief Sets the arguments that go into the python udf function
     * @param pythonArguments arguments for the udf function as a PyObject
     */
    void setPythonArguments(PyObject* pythonArguments) { this->pythonArguments = pythonArguments; }

    /**
     * @brief Getter function for the arguments that go into the python udf function
     * @return returns python arguments as a PyObject
     */
    PyObject* getPythonVariable() const { return this->pythonVariable; }

    /**
     * @brief Sets the arguments that go into the python udf function
     * @param pythonArguments arguments for the udf function as a PyObject
     */
    void setPythonVariable(PyObject* pythonVariable) { this->pythonVariable = pythonVariable; }
    /**
     * @brief Getter function for the python function
     * @return the python function as a PyObject
     */
    PyObject* getPythonFunction() const { return this->pythonFunction; }

    /**
     * @brief Getter function for the python module
     * @return python module as a PyObject
     */
    PyObject* getPythonModule() const { return this->pythonModule; }

    /**
     * @brief Initializes the python udf in a module
     */
    void initPython();

    /**
     * @brief Undo all initialization of the python interpreter
     */
    void finalize();

    void start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) override {}
    void stop(QueryTerminationType, PipelineExecutionContextPtr) override {}

  private:
    const std::string function;
    const std::string functionName;
    const std::map<std::string, std::string> modulesToImport;
    const std::string pythonCompiler;
    const SchemaPtr inputSchema;
    const SchemaPtr outputSchema;
    std::string moduleName;
    PyObject* pythonArguments;// arguments of python user defined function
    PyObject* pythonFunction; // python function object
    PyObject* pythonModule;   // python module object
    PyObject* pythonVariable; // temp python variable for setting arguments
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPPYTHONUDFOPERATORHANDLER_HPP_
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFOPERATORHANDLER_HPP_
