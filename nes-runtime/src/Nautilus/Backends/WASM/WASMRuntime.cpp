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
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Nautilus/Backends/WASM/WASMRuntime.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <utility>

using namespace wasmtime;

namespace NES::Nautilus::Backends::WASM {

WASMRuntime::WASMRuntime() : linker(engine), store(engine) {
    config.wasm_memory64(true);
    engine = Engine(std::move(config));
    linker = Linker(engine);
    store = Store(engine);
    auto pyWat = parseWATFile("/home/victor/wanes-engine/python/python3.11.wat");
    pyModule.emplace_back(wasmtime::Module::compile(engine, pyWat).unwrap());

    auto isPreOpened = wasiConfig.preopen_dir("/home/victor/wanes-engine/python", ".");
    if (!isPreOpened) {
        NES_ERROR("Could not pre-open directory for python")
    }
    wasiConfig.inherit_env();
    wasiConfig.inherit_stdin();
    wasiConfig.inherit_stdout();
    wasiConfig.inherit_stderr();
    store.context().set_wasi(std::move(wasiConfig)).unwrap();
    linker.define_wasi().unwrap();

    prepareCPython();
}

void WASMRuntime::setup(const std::vector<std::string>& proxyFunctions) {
    for (const auto& proxy : proxyFunctions) {
        linkHostFunction(proxy);
    }
}

int32_t WASMRuntime::run(size_t binaryLength, char* queryBinary) {
    //TODO: Find out if wat or wasm format is passed
    wasmtime::Span query{(uint8_t*) queryBinary, binaryLength};
    auto module = wasmtime::Module::compile(engine, queryBinary).unwrap();

    auto instance = linker.instantiate(store, module).unwrap();

    auto execute = std::get<Func>(*instance.get(store, "execute"));

    auto results = execute.call(store, {}).unwrap();
    int32_t res = 0;
    if (!results.empty()) {
        res = results[0].i32();
    }
    return res;
}

void WASMRuntime::linkHostFunction(const std::string& proxyFunctionName) {
    if (proxyFunctionName == "NES__Runtime__TupleBuffer__getBuffer") {
        host_NES__Runtime__TupleBuffer__getBuffer(proxyFunctionName);
    } else if (proxyFunctionName == "NES__Runtime__TupleBuffer__getBufferSize") {
        host_NES__Runtime__TupleBuffer__getBufferSize(proxyFunctionName);
    } else if (proxyFunctionName == "NES__Runtime__TupleBuffer__getNumberOfTuples") {
        host_NES__Runtime__TupleBuffer__getNumberOfTuples(proxyFunctionName);
    } else if (proxyFunctionName == "NES__Runtime__TupleBuffer__setNumberOfTuples") {
        host_NES__Runtime__TupleBuffer__setNumberOfTuples(proxyFunctionName);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void WASMRuntime::prepareCPython() {
    /**
     * This host function overwrites the WASI function args_sizes_get which is called by the cpython wasm interpreter.
     * With this, we "trick" cpython into thinking that we have at least 1 function argument.
     */
    auto argsSizesGet = linker.func_new("cpython",
                                        "args_sizes_get",
                                        FuncType({ValKind::I32, ValKind::I32}, {ValKind::I32}),
                                        [](auto caller, auto params, auto results) {
                                            (void) caller;
                                            (void) params;
                                            results[0] = 0;
                                            return std::monostate();
                                        });

    /**
     * Same as for args_sizes_get.
     */
    auto argsGet = linker.func_new("cpython",
                                   "args_get",
                                   FuncType({ValKind::I32, ValKind::I32}, {ValKind::I32}),
                                   [](auto caller, auto params, auto results) {
                                       (void) caller;
                                       (void) params;
                                       results[0] = 0;
                                       return std::monostate();
                                   });

    /**
     * This host function is called before python is invoked. It writes the arguments into a args file, which the python UDF
     * then accesses.
     * For now allow UDFs with two arguments of type i32...
     */
    auto prepareArgs = linker.func_new("cpython",
                                       "prepare_args",
                                       FuncType({ValKind::I32, ValKind::I32}, {}),
                                       [](auto caller, auto params, auto results) {
                                           (void) caller;
                                           (void) results;
                                           auto param0 = params[0].i32();
                                           auto param1 = params[1].i32();
                                           std::ofstream file("/home/victor/wanes-engine/python/args.txt");
                                           if (file.is_open()) {
                                               file << param0 << " " << param1;
                                               file.close();
                                           } else {
                                               NES_ERROR("Python args file could not be opened")
                                           }
                                           return std::monostate();
                                       });
    /**
     * This function actually calls the cpython wasm interpreter and executes a python UDF
     */
    auto prepareCPython = linker.func_new("cpython", "cpython", FuncType({}, {ValKind::I32}), [&](auto caller, auto params, auto results) {
        (void) caller;
        (void) params;
        (void) results;
        auto pyInstance = linker.instantiate(store, pyModule.back()).unwrap();
        auto pyExecute = std::get<Func>(*pyInstance.get(store, "_start"));
        auto funcResult = pyExecute.call(store, {}).unwrap();
        std::ifstream file("/home/victor/wanes-engine/python/args.txt");
        std::string res;
        if (file.is_open()) {
            getline(file, res);
            results[0] = stoi(res);
            file.close();
        } else {
            NES_ERROR("Python args file could not be opened");
        }
        return std::monostate();
    });
}

void WASMRuntime::host_NES__Runtime__TupleBuffer__getBuffer(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule,
                               proxyFunctionName,
                               FuncType({ValKind::I32}, {ValKind::I32}),
                               [](auto caller, auto params, auto results) {
                                   auto ptr1 = params[0].i32();
                                   auto mem = std::get<Memory>(*caller.get_export("memory"));

                                   //auto res = NES::Nautilus::RecordBuffer::NES__Runtime__TupleBuffer // ::NES__Runtime__TupleBuffer__getBuffer(nullptr);
                                   results[0] = 0;
                                   return std::monostate();
                               });
}

/*uint64_t*/ void WASMRuntime::host_NES__Runtime__TupleBuffer__getBufferSize(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule,
                               proxyFunctionName,
                               FuncType({ValKind::I32}, {ValKind::I64}),
                               [](auto caller, auto params, auto results) {
                                   auto ptr1 = params[0].i32();
                                   auto mem = std::get<Memory>(*caller.get_export("memory"));
                                   //int32_t *ptr3 = ptr1;
                                   //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBufferSize(ptr3);
                                   results[0] = 0;
                                   return std::monostate();
                               });
};
/*uint64_t*/ void WASMRuntime::host_NES__Runtime__TupleBuffer__getNumberOfTuples(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule,
                               proxyFunctionName,
                               FuncType({ValKind::I32}, {ValKind::I64}),
                               [](auto caller, auto params, auto results) {
                                   auto ptr1 = params[0].i32();
                                   auto mem = std::get<Memory>(*caller.get_export("memory"));
                                   //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples(ptr3);
                                   results[0] = 0;
                                   return std::monostate();
                               });
};
void WASMRuntime::host_NES__Runtime__TupleBuffer__setNumberOfTuples(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule,
                               proxyFunctionName,
                               FuncType({ValKind::I32, ValKind::I64}, {}),
                               [](auto caller, auto params, auto results) {
                                   auto ptr1 = params[0].i32();
                                   auto mem = std::get<Memory>(*caller.get_export("memory"));
                                   //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples(ptr3);
                                   results[0] = 0;
                                   return std::monostate();
                               });
}

std::string WASMRuntime::parseWATFile(const char* fileName) {
    std::ifstream watFile;
    watFile.open(fileName);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    return strStream.str();
}

}// namespace NES::Nautilus::Backends::WASM