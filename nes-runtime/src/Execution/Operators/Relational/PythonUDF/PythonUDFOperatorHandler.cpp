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

#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <string>

namespace NES::Runtime::Execution::Operators {

void PythonUDFOperatorHandler::initPython() {
    this->moduleName = this->functionName + "Module";
    // initialize python interpreter
    Py_Initialize();
    // apparently we only need to add this decorator
    std::string testNumbaExec = "from numba import jit"
                                "\n"
                                "@jit(nopython=True)\n" + this->function;
    PyObject* pythonCode = Py_CompileString(testNumbaExec.c_str(), "", Py_file_input);
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
}

void PythonUDFOperatorHandler::finalize() {
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

}// namespace NES::Runtime::Execution::Operators

#endif// NAUTILUS_PYTHON_UDF_ENABLED