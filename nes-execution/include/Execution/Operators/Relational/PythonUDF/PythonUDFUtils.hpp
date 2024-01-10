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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFUTILS_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFUTILS_HPP_

namespace NES::Runtime::Execution::Operators {

/**
     * @brief Checks for an exception in the python Interpreter and throws a runtime error if one is found.
     *
     * @param pyObject Python Object
     * @param func_name name of the function where the error occurred: should be __func__
     * @param line_number line number where the error occurred: should be __LINE__
     * @param errorMessage error message that is shown
     */
inline void pythonInterpreterErrorCheck(PyObject* pyObject, const char* func_name, int line_number, std::string errorMessage) {
    if (pyObject == NULL) {
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }
        NES_THROW_RUNTIME_ERROR("[" << func_name << ": line " << line_number << "] " << errorMessage);
    }
}
};// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFUTILS_HPP_
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_PYTHONUDFUTILS_HPP_
