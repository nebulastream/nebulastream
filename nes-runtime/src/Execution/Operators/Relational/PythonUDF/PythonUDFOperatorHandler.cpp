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
#include <filesystem>
#include <fstream>
#include <string>

namespace NES::Runtime::Execution::Operators {

PythonUDFOperatorHandler::PythonUDFOperatorHandler(const std::string& function,
                         const std::string& functionName,
                         const std::map<std::string, std::string> modulesToImport,
                         const std::string& pythonCompiler,
                         SchemaPtr inputSchema,
                         SchemaPtr outputSchema,
                         Timer<>& pythonUDFCompilationTimeTimer)
    : function(function), functionName(functionName), modulesToImport(modulesToImport), pythonCompiler(pythonCompiler), inputSchema(inputSchema), outputSchema(outputSchema), pythonUDFCompilationTimeTimer(pythonUDFCompilationTimeTimer){
    this->initPython();
}

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

std::string PythonUDFOperatorHandler::getPyPyDataType(AttributeFieldPtr& fieldDataType) {
    // https://numba.pydata.org/numba-doc/latest/reference/types.html
    // it looks like there are no for strings?
    if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
        return "bool";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createInt8())) {
        return "char";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createInt16())) {
        return "short";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createInt32())) {
        return "int";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createInt64())) {
        return "long";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createFloat())) {
        return "float";
    } else if (fieldDataType->getDataType()->isEquals(DataTypeFactory::createDouble())) {
        return "double";
    } else {
        return "";
    }
}

std::string PythonUDFOperatorHandler::generatePyPyFunctionDeclaration(){
    // int do_stuff(int, int);
    std::string pypyFunctionDeclarationString = "";
    if (outputSchema->fields.size() == 1) {
        auto field = outputSchema->fields[0];
        pypyFunctionDeclarationString += this->getPyPyDataType(field) + " " + this->functionName + "(";
    }

    for (int i = 0; i < (int) inputSchema->fields.size(); i++) {
        auto field = inputSchema->fields[i];
        pypyFunctionDeclarationString += this->getPyPyDataType(field) + ", ";
    }

    // remove last two characters, a whitespace and a comma
    pypyFunctionDeclarationString.erase(pypyFunctionDeclarationString.length()-2);
    pypyFunctionDeclarationString += ")";
    return pypyFunctionDeclarationString;
}

void PythonUDFOperatorHandler::generatePythonFile(std::string path, std::string file, std::string pythonCode) {
    // create .pyx file
    // add python code into that file
    if (!std::filesystem::is_directory(path)) {
        std::filesystem::create_directory(path);
    }

    std::ofstream pyxFile;
    pyxFile.open(file, std::ios::trunc);
    pyxFile << pythonCode << "\n";
    pyxFile.close();

}

void PythonUDFOperatorHandler::importCompiledPythonModule(std::string path) {
    // now we can use it like a normal module
    // add path to PYTHONPATH
    // do this: sys.path.append('/path/to/mod_directory')
    auto addPath = "import sys\nsys.path.append(\"" + path + "\")\n";
    PyRun_SimpleString(addPath.c_str());
    // open module and use function
    this->pythonModule = PyImport_ImportModule(this->functionName.c_str());

    if (this->pythonModule == NULL) {
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
            NES_THROW_RUNTIME_ERROR("Importing the module didn't work out");
        }
    }
}

std::string PythonUDFOperatorHandler::getModulesToImportAsString() {
    std::string pythonCode = "";
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
    return pythonCode;
}

void PythonUDFOperatorHandler::initPython() {
    pythonUDFCompilationTimeTimer.start();
    this->moduleName = this->functionName + "Module";
    // initialize python interpreter
    Py_Initialize();
    std::string pythonCode = getModulesToImportAsString();
    //choose python compiler, default is the CPython compiler
    if (this->pythonCompiler == "numba") {
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
                      "\n" +
                      getModulesToImportAsString() +
                      "\n"
                      "@numba.cfunc(\"" + numbaSignature + "\", nopython=True)\n" +
                      this->function + "\n" +
                      this->functionName + "_address = " + this->functionName + ".address";
        //PyRun_String(pythonCode.c_str(), Py_file_input, globals, locals);
    } else {
        // default, just using the CPython compiler or Cython or Nuitka
        pythonCode += this->function;
    }

    if (pythonCompiler == "cython") {
        // create .pyx file
        // add python code into that file
        std::string path = std::filesystem::current_path().string() + std::filesystem::path::preferred_separator + "tmp";
        auto file = path + std::filesystem::path::preferred_separator + this->functionName + ".pyx";
        this->generatePythonFile(path, file, pythonCode);

        // build cython module
        // we generate a .c file with cython
        // --embed option so that we dont have to do all the init stuff in CPython our own
        // -3 means we're generating Python 3 syntax
        // cython --embed -3 myownmodule.pyx
        auto generateCFile = "cython --embed -3 " + file;
        std::system(generateCFile.c_str());

        std::string cFile = path + std::filesystem::path::preferred_separator + this->functionName + ".c";
        std::string oFile = path + std::filesystem::path::preferred_separator + this->functionName + ".o";

        // compile .c file
        auto generateOFile = "gcc -pthread -fPIC -I/usr/include/python3.10 -g -c " + cFile + " -o " + oFile;
        std::system(generateOFile.c_str());

        // link file against the Python library
        auto generateSOFile = "gcc " + oFile + " -fPIC -pthread -g -shared -o " + path
            + std::filesystem::path::preferred_separator + this->functionName + ".so `python3-config --libs`";
        std::system(generateSOFile.c_str());

        // remove files we don't need anymore
        std::remove(cFile.c_str());
        std::remove(oFile.c_str());

        this->importCompiledPythonModule(path);
    } else if (pythonCompiler == "nuitka") {
        // create .pyx file
        // add python code into that file
        auto path = std::filesystem::current_path().string() + std::filesystem::path::preferred_separator + "tmp";
        auto file = path + std::filesystem::path::preferred_separator + this->functionName + ".py";
        this->generatePythonFile(path, file, pythonCode);

        // python3 -m nuitka --module some_module.py
        auto generateSOFile = "python3 -m nuitka --plugin-enable=numpy --module " + path + std::filesystem::path::preferred_separator + this->functionName + ".py";
        std::system(generateSOFile.c_str());

        this->importCompiledPythonModule(path);
    } else if (pythonCompiler == "pypy") {
        // generate c function declaration

        std::string functionDeclaration = this->generatePyPyFunctionDeclaration();
        auto path = std::filesystem::current_path().string() + std::filesystem::path::preferred_separator + "tmp";
        auto file = path + std::filesystem::path::preferred_separator + this->functionName + ".py";

        // generate py file that creates shared library with cffi
        std::string pythonFile = "import cffi\n"
                                 "ffibuilder = cffi.FFI()\n"
                                 "\n"
                                 "ffibuilder.embedding_api(\"\"\"\n"
                                 "     " + functionDeclaration + ";\n"
                                 "\"\"\")\n"
                                 "\n"
                                 "ffibuilder.set_source(\"udfs\", \"\", extra_link_args=['-Wl,-rpath=/home/hpham/pypy2.7-v7.3.13-linux64/bin'])\n"
                                 "\n"
                                 "ffibuilder.embedding_init_code(\"\"\"\n"
                                 "from udfs import ffi\n"
                                 + this->getModulesToImportAsString() + "\n"
                                 "\n"
                                 "@ffi.def_extern()\n"
                                  + this->function +"\n"
                                 "\"\"\")\n"
                                 "\n"
                                 "ffibuilder.compile(target=\"udfs.*\", verbose=True)";


        this->generatePythonFile(path, file, pythonFile);

        // run cffiEmbedding with pypy
        auto generateSOFile = "$HOME/pypy2.7-v7.3.13-linux64/bin/pypy " + file;
        std::system(generateSOFile.c_str());

        //auto soFile = path + std::filesystem::path::preferred_separator + "udfs.so";

    } else {
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
    pythonUDFCompilationTimeTimer.snapshot("init");
    pythonUDFCompilationTimeTimer.pause();
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

void PythonUDFOperatorHandler::addSumExecution(double printTime) {
    sumExecution += printTime;
}

}// namespace NES::Runtime::Execution::Operators

#endif// NAUTILUS_PYTHON_UDF_ENABLED