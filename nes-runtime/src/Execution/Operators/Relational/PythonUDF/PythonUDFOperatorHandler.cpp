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

    std::string pythonCode;
    if (!this->modulesToImport.empty()) {
        // import modules
        std::map<std::string, std::string>::const_iterator it = this->modulesToImport.begin();

        while (it != this->modulesToImport.end()){
            pythonCode = "import " + it->first; // creates the "import module_name" string
            if (it->second != "") {
                pythonCode += " as " + it->second; // adds "as alias_module_name"
            }
            pythonCode += "\n"; // new line bc we are going to add the function string right after this
            //PyObject *pyImportCode = Py_CompileString(importCode.c_str(), "", Py_file_input);
            // add import of libraries to module
            //this->pythonModule = PyImport_ExecCodeModule(this->moduleName.c_str(), pyImportCode);
            /*if (this->pythonModule == NULL) {
                if (PyErr_Occurred()) {
                    PyErr_Print();
                    PyErr_Clear();
                }
                NES_THROW_RUNTIME_ERROR("Cannot add import " << importCode << " to module " << this->moduleName);
            }*/
            ++it;
        }
    }

    //choose python compiler, default is the CPython compiler
    if (this->pythonCompiler == "numba") {
        pythonCode = "from numba import jit"
                      "\n"
                      "@jit(nopython=True)\n";
        /*PyObject* compiledNumbaImport = Py_CompileString(numbaImport.c_str(), "", Py_file_input);
        if (compiledNumbaImport == NULL) {
            if (PyErr_Occurred()) {
                PyErr_Print();
                PyErr_Clear();
                NES_THROW_RUNTIME_ERROR("Could not compile numba import.");
            }
        }*/
    }

    // compile function string
    pythonCode += this->function;
    PyObject* compiledPythonCode = Py_CompileString(pythonCode.c_str(), "", Py_file_input);
    if (compiledPythonCode == NULL) {
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
            NES_THROW_RUNTIME_ERROR("Could not compile the python code");
        }
    }

    // add python code into our module
    this->pythonModule = PyImport_ExecCodeModule(this->moduleName.c_str(), compiledPythonCode);
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