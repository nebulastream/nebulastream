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

WASMRuntime::WASMRuntime() : linker(engine), store(engine) {}

void WASMRuntime::setup(const std::vector<std::string>& proxyFunctions) {
    config.wasm_memory64(true);
    engine = Engine(std::move(config));
    linker = Linker(engine);
    store = Store(engine);
    /*
    auto isPreOpened = wasiConfig.preopen_dir("/home/victor/wanes-engine/python", ".");
    if (!isPreOpened) {
        NES_ERROR("Could not pre-open directory for python");
    }
     */
    wasiConfig.inherit_env();
    wasiConfig.inherit_stdin();
    wasiConfig.inherit_stdout();
    wasiConfig.inherit_stderr();
    store.context().set_wasi(std::move(wasiConfig)).unwrap();
    linker.define_wasi().unwrap();

    for (const auto& proxy : proxyFunctions) {
        linkHostFunction(proxy);
    }
    prepareCPython();
}

void WASMRuntime::run(size_t binaryLength, char* queryBinary) {

    config.wasm_memory64(true);
    engine = Engine(std::move(config));
    linker = Linker(engine);
    store = Store(engine);
    /*
    auto isPreOpened = wasiConfig.preopen_dir("/home/victor/wanes-engine/python", ".");
    if (!isPreOpened) {
        NES_ERROR("Could not pre-open directory for python");
    }
     */
    wasiConfig.inherit_env();
    wasiConfig.inherit_stdin();
    wasiConfig.inherit_stdout();
    wasiConfig.inherit_stderr();
    store.context().set_wasi(std::move(wasiConfig)).unwrap();
    linker.define_wasi().unwrap();

    wasmtime::Span query{(uint8_t*) queryBinary, binaryLength};
    auto wat = parseWATFile("/home/victor/sketch.wat");
    auto pyWat = parseWATFile("/home/victor/wanes-engine/python/python3.11.wat");

    auto module = wasmtime::Module::compile(engine, wat).unwrap();
    auto pyModule = wasmtime::Module::compile(engine, pyWat).unwrap();

    auto instance = linker.instantiate(store, module).unwrap();
    auto pyInstance = linker.instantiate(store, pyModule).unwrap();

    auto execute = std::get<Func>(*instance.get(store, "execute"));
    pyExecute = std::get<Func>(*pyInstance.get(store, "_start"));

    auto memory = std::get<Memory>(*instance.get(store, "memory"));
    pyMemory = std::get<Memory>(*pyInstance.get(store, "memory"));

    auto results = execute.call(store, {}).unwrap();
    //std::cout << results[0].i32() << "\n";
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
                                               NES_ERROR("Python args file could not be opened");
                                           }
                                           return std::monostate();
                                       });

    auto prepareCPython = linker.func_new("cpython",
                                       "cpython",
                                       FuncType({}, {}),
                                       [&](auto caller, auto params, auto results) {
                                           (void) caller;
                                           (void) params;
                                           (void) results;
                                           auto data = pyMemory.data(caller);
                                           for (size_t i = 0; i < 7; ++i) {
                                               data[i] = udfFileName[i];
                                           }
                                           for (size_t i = 0; i < 10; i++)
                                           {
                                               std::cout << "MEM " << i << " - " << (char)pyMemory.data(store)[i] << "\n";
                                           }
                                           auto funcResult = pyExecute.call(store, {}).unwrap();
                                           return std::monostate();
                                       });

}

void WASMRuntime::host_NES__Runtime__TupleBuffer__getBuffer(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule,
                               proxyFunctionName,
                               FuncType({ValKind::I32}, {ValKind::I32}),
                               [](auto caller, auto params, auto results) {
                                   auto ptr1 = params[0].i32();
                                   auto ptr2 = params[1].i32();
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
                                   auto ptr2 = params[1].i32();
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
                                   auto ptr2 = params[1].i32();
                                   auto mem = std::get<Memory>(*caller.get_export("memory"));
                                   //int32_t *ptr3 = ptr1;
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
                                   auto ptr2 = params[1].i32();
                                   auto mem = std::get<Memory>(*caller.get_export("memory"));
                                   //int32_t *ptr3 = ptr1;
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

wasmtime::Span<uint8_t> WASMRuntime::parseWASMFile(const char* fileName) {
    std::ifstream watFile;
    watFile.open(fileName);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    auto str = strStream.str();
    auto ss = str.size();
    auto cc = str.c_str();
    wasmtime::Span query{(uint8_t*) cc, ss};
    return query;
}

}// namespace NES::Nautilus::Backends::WASM