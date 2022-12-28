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
    auto t = linker.define_wasi();
    store = Store(engine);

    auto tmp = wasiConfig.preopen_dir("/home/victor/wanes-engine/python", ".");
    store.context().set_wasi(std::move(wasiConfig)).unwrap();

    for (const auto& proxy : proxyFunctions) {
        linkHostFunction(proxy);
    }
    prepareCPython();
}

void WASMRuntime::run(size_t binaryLength, char* queryBinary) {
    wasmtime::Span query{(uint8_t*) queryBinary, binaryLength};
    auto wat = parseWATFile("/home/victor/sketch.wat");

    auto module = wasmtime::Module::compile(engine, wat).unwrap();

    Instance instance = linker.instantiate(store, module).unwrap();
    auto execute = std::get<wasmtime::Func>(*instance.get(store, "execute"));
    auto memory = std::get<Memory>(*instance.get(store, "memory"));
    Span<uint8_t> memSpan = memory.data(store.context());
    auto memDataPtr = memSpan.data();

    auto results = execute.call(store, {}).unwrap();
    std::cout << results[0].i32() << "\n";
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

    auto res = linker.func_new("cpython",
                               "cpython",
                               FuncType({ValKind::I32, ValKind::I32}, {ValKind::I32}),
                               [this](auto caller, auto params, auto results) {
                                   auto ptr1 = params[0].i32();
                                   auto mem = std::get<Memory>(*caller.get_export("memory"));
                                   WasiConfig conf;
                                   auto tmp = conf.preopen_dir("/home/victor/wanes-engine/python", ".");
                                   std::cout << "Preopen: " << tmp << "\n";
                                   std::vector<std::basic_string<char>> args;
                                   args.emplace_back("add.py");
                                   //args.emplace_back("5");
                                   //args.emplace_back("7");
                                   conf.argv(args);
                                   conf.inherit_env();
                                   conf.inherit_stdin();
                                   conf.inherit_stderr();
                                   conf.inherit_stdout();
                                   store.context().set_wasi(std::move(conf)).unwrap();

                                   std::ifstream watFile;
                                   watFile.open(cpythonFilePath);
                                   std::stringstream strStream;
                                   strStream << watFile.rdbuf();

                                   auto wat = strStream.str();
                                   auto ss = wat.size();
                                   auto cc = wat.c_str();
                                   Span q{(uint8_t*) cc, ss};

                                   auto module = wasmtime::Module::compile(engine, q).unwrap();

                                   Instance instance = linker.instantiate(store, module).unwrap();
                                   auto execute = std::get<wasmtime::Func>(*instance.get(store, "_start"));

                                   auto res = execute.call(store, {}).unwrap();

                                   results[0] = 0;//res[0].i32();
                                   return std::monostate();
                               });
}

void WASMRuntime::hostFunction() {
    auto res = linker.func_new(proxyFunctionModule, "test", FuncType({}, {}), [](auto caller, auto params, auto results) {
        auto mem = std::get<Memory>(*caller.get_export("memory"));
        (void) params;
        (void) results;

        mem.data(caller)[8] = (int8_t) 'h';
        //results[0] = res;
        for (int i = 0; i < 15; ++i) {
            std::cout << "mem: " << static_cast<int>(mem.data(caller)[i]) << "\n";
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