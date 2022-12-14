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
#include <Nautilus/Backends/WASM/WASMRuntime.hpp>
#include <Experimental/Interpreter/ProxyFunctions.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <utility>

using namespace wasmtime;

namespace NES::Nautilus::Backends::WASM {

WASMRuntime::WASMRuntime() : linker(engine), store(engine) {}

void WASMRuntime::setup(const std::vector<std::string>& proxyFunctions) {
    for (const auto& proxy : proxyFunctions) {
        linkHostFunction(proxy);
    }
}

void WASMRuntime::run(size_t binaryLength, char *queryBinary) {
    wasmtime::Span query{(uint8_t*) queryBinary, binaryLength};

    auto module = wasmtime::Module::compile(engine, query).unwrap();

    /*
    Func args_size(&store, FuncType({ValKind::I32, ValKind::I32}, {ValKind::I32}), [](auto caller, auto params, auto results) {
        auto ptr1 = params[0].i32();
        auto ptr2 = params[1].i32();
        auto mem = std::get<Memory>(*caller.get_export("memory"));
        mem.data(caller)[ptr1];
        results[0] = ptr1;
        return std::monostate();
    });

    Func args_get(&store, FuncType({ValKind::I32, ValKind::I32}, {ValKind::I32}), [](auto caller, auto params, auto results) {
        auto ptr = params[0].i32();
        Memory mem = std::get<Memory>(*caller.get_export("memory"));
        wasmtime::Span<uint8_t> d = mem.data(caller);
        results[0] = ptr;
        return std::monostate();
    });
    */

    auto instance = wasmtime::Instance::create(store, module, {}).unwrap();
    auto execute = std::get<wasmtime::Func>(*instance.get(store, "execute"));

    auto results = execute.call(store, {5}).unwrap();
    std::cout << results[0].i32();
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

/*void**/ void WASMRuntime::host_NES__Runtime__TupleBuffer__getBuffer(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule, proxyFunctionName, FuncType({ValKind::I32}, {ValKind::I32}), [](auto caller, auto params, auto results) {
        auto ptr1 = params[0].i32();
        auto ptr2 = params[1].i32();
        auto mem = std::get<Memory>(*caller.get_export("memory"));
        //int32_t *ptr3 = ptr1;
        //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer(ptr3);
        results[0] = 0;
        return std::monostate();
    });
};
/*uint64_t*/ void WASMRuntime::host_NES__Runtime__TupleBuffer__getBufferSize(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule, proxyFunctionName, FuncType({ValKind::I32}, {ValKind::I64}), [](auto caller, auto params, auto results) {
        auto ptr1 = params[0].i32();
        auto ptr2 = params[1].i32();
        auto mem = std::get<Memory>(*caller.get_export("memory"));
        //int32_t *ptr3 = ptr1;
        //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer(ptr3);
        results[0] = 0;
        return std::monostate();
    });
};
/*uint64_t*/ void WASMRuntime::host_NES__Runtime__TupleBuffer__getNumberOfTuples(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule, proxyFunctionName, FuncType({ValKind::I32}, {ValKind::I64}), [](auto caller, auto params, auto results) {
        auto ptr1 = params[0].i32();
        auto ptr2 = params[1].i32();
        auto mem = std::get<Memory>(*caller.get_export("memory"));
        //int32_t *ptr3 = ptr1;
        //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer(ptr3);
        results[0] = 0;
        return std::monostate();
    });
};
void WASMRuntime::host_NES__Runtime__TupleBuffer__setNumberOfTuples(const std::string& proxyFunctionName) {
    auto res = linker.func_new(proxyFunctionModule, proxyFunctionName, FuncType({ValKind::I32, ValKind::I64}, {}), [](auto caller, auto params, auto results) {
        auto ptr1 = params[0].i32();
        auto ptr2 = params[1].i32();
        auto mem = std::get<Memory>(*caller.get_export("memory"));
        //int32_t *ptr3 = ptr1;
        //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer(ptr3);
        results[0] = 0;
        return std::monostate();
    });
}


void WASMRuntime::prepareCPython() {

}

std::string WASMRuntime::parseWASMFile(const char* fileName) {
    std::ifstream watFile;
    watFile.open(fileName);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    return strStream.str();
}

}// namespace NES::Nautilus::Backends::WASM