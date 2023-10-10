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

std::string PythonUDFOperatorHandler::getNumbaDataType(AttributeFieldPtr& fieldDataType) {
    // https://numba.pydata.org/numba-doc/latest/reference/types.html
    // it looks like there are no for strings?
    if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
        return "boolean";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createInt8())) {
        return "int8";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createInt16())) {
        return "int16";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createInt32())) {
        return "int32";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createInt64())) {
        return "int64";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createFloat())) {
        return "float32";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createDouble())) {
        return "float64";
    } else {
        return "";
    }
}

std::string PythonUDFOperatorHandler::getNumbaSignature(){
    // needs to look like this in the end: "float64(float64, float64)"
    // or "Tuple((float64, float64))(float64, float64)"
    std::string numbaInputSignatureString = "(";
    for (int i = 0; i < (int) inputSchema->fields.size(); i++) {
        auto field = inputSchema->fields[i];
        numbaInputSignatureString += this->getNumbaDataType(field) + ", ";
    }
    // remove last two characters, a whitespace and a comma
    numbaInputSignatureString.erase(numbaInputSignatureString.length()-2);
    numbaInputSignatureString += ")";

    // the output has to look sth like this:
    // multiple return variables Tuple((float64, float64))
    // one return variable float64
    std::string numbaOutputSignatureString = "";
    if (outputSchema->fields.size() == 1) {
        auto field = outputSchema->fields[0];
        numbaOutputSignatureString += this->getNumbaDataType(field);
    } else {
        numbaOutputSignatureString += "Tuple((";
        for (int i = 0; i < (int) outputSchema->fields.size(); i++) {
            auto field = outputSchema->fields[i];
            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                numbaOutputSignatureString += this->getNumbaDataType(field) + ", ";
            }
        }
        numbaOutputSignatureString.erase(numbaOutputSignatureString.length()-2);
        numbaOutputSignatureString += "))";
    }
    return numbaOutputSignatureString + numbaInputSignatureString;
}

void PythonUDFOperatorHandler::initPython() {
    this->moduleName = this->functionName + "Module";
    // initialize python interpreter
    Py_Initialize();
    std::string pythonCode = "";
    //choose python compiler, default is the CPython compiler
    if (this->pythonCompiler == "numba"){
        dyncall.reset();
        // init globals and locals to be able to access the variables later when calling the function
        // they have to be a pyDict
        globals = PyDict_New();
        locals = PyDict_New();
        // should look like this
        // import numba
        //
        // @numba.cfunc(numbaSignature, nopython=True),
        // udf_function
        //
        // udf_function_address = udf_function.address
        std::string numbaSignature = this->getNumbaSignature();
        pythonCode += "import numba"
                      "\n"
                      "@numba.cfunc(\"" + numbaSignature + "\", nopython=True)\n" +
                      this->function +
                      "\n" +
                      this->functionName + "_address = " + this->functionName + ".address";
        /*pythonCode += "from numba.pycc import CC\n"
                      "\n"
                      "cc = CC('"+ this->moduleName +"')\n"
                      "# Uncomment the following line to print out the compilation steps\n"
                      "cc.verbose = True\n"
                      "\n"
                      "@cc.export('"+ this->functionName+"', '" + numbaSignature + "')\n"
                      + this->function +
                      "\n"
                      "if __name__ == \"__main__\":\n"
                      "    cc.compile()";*/
        //PyRun_String(pythonCode.c_str(), Py_file_input, globals, locals);
        //auto addressVariableName = this->functionName + "_address";
        //auto addressAsStringPyObject = PyDict_GetItemString(locals, addressVariableName.c_str());
        //uintptr_t functionAddress = PyLong_AsUnsignedLongLong(addressAsStringPyObject);
        //std::cout << "HANDLER function address (0x" << std::hex << functionAddress << std::endl << std::flush;
    } else {
        // default, just using the CPython compiler
        pythonCode += this->function;
    }
        // compile function string
        PyObject* compiledPythonCode = Py_CompileString(pythonCode.c_str(), "", Py_file_input);
        if (compiledPythonCode == NULL) {
            if (PyErr_Occurred()) {
                PyErr_Print();
                PyErr_Clear();
                NES_THROW_RUNTIME_ERROR("Could not compile function string.");
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
    Py_DecRef(this->pythonFunction);
    Py_DecRef(this->globals);
    Py_DecRef(this->locals);

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