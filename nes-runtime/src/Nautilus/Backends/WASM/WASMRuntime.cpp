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
for (const auto& proxy : proxyFunctions) {
    linkHostFunction(proxy);
}
hostFunction();
}

void WASMRuntime::run(size_t binaryLength, char *queryBinary) {
wasmtime::Span query{(uint8_t*) queryBinary, binaryLength};
auto wat = parseWASMFile("/home/victor/sketch.wat");

auto module = wasmtime::Module::compile(engine, wat).unwrap();

Instance instance = linker.instantiate(store, module).unwrap();
auto execute = std::get<wasmtime::Func>(*instance.get(store, "execute"));

auto results = execute.call(store, {}).unwrap();
std::cout << results[0].i64() << "\n";
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

static int add() {
    int x = 4;
    int y = 10;
    return x + y;
}

void WASMRuntime::hostFunction() {
    auto res = linker.func_new(proxyFunctionModule, "test", FuncType({}, {}), [](auto caller, auto params, auto results) {
        auto mem = std::get<Memory>(*caller.get_export("memory"));
        (void)params;
        (void)results;
        mem.data(caller)[8] = (int8_t)'h';
        auto res = add();
        //results[0] = res;
        for (int i = 0; i < 15; ++i) {
            std::cout << "mem: " << static_cast<int>(mem.data(caller)[i]) << "\n";
        }

        return std::monostate();
    });
}

void WASMRuntime::host_NES__Runtime__TupleBuffer__getBuffer(const std::string& proxyFunctionName) {
auto res = linker.func_new(proxyFunctionModule, proxyFunctionName, FuncType({ValKind::I32}, {ValKind::I32}), [](auto caller, auto params, auto results) {
    auto ptr1 = params[0].i32();
    auto ptr2 = params[1].i32();
    auto mem = std::get<Memory>(*caller.get_export("memory"));

    //auto res = NES::Nautilus::RecordBuffer::NES__Runtime__TupleBuffer // ::NES__Runtime__TupleBuffer__getBuffer(nullptr);
    results[0] = 0;
    return std::monostate();
});
}

/*uint64_t*/ void WASMRuntime::host_NES__Runtime__TupleBuffer__getBufferSize(const std::string& proxyFunctionName) {
auto res = linker.func_new(proxyFunctionModule, proxyFunctionName, FuncType({ValKind::I32}, {ValKind::I64}), [](auto caller, auto params, auto results) {
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
auto res = linker.func_new(proxyFunctionModule, proxyFunctionName, FuncType({ValKind::I32}, {ValKind::I64}), [](auto caller, auto params, auto results) {
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
auto res = linker.func_new(proxyFunctionModule, proxyFunctionName, FuncType({ValKind::I32, ValKind::I64}, {}), [](auto caller, auto params, auto results) {
    auto ptr1 = params[0].i32();
    auto ptr2 = params[1].i32();
    auto mem = std::get<Memory>(*caller.get_export("memory"));
    //int32_t *ptr3 = ptr1;
    //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples(ptr3);
    results[0] = 0;
    return std::monostate();
});
}


void WASMRuntime::prepareCPython() {
auto res = linker.func_new("cpython", "cpython", FuncType({ValKind::I32, ValKind::I32}, {ValKind::I32}), [](auto caller, auto params, auto results) {
    auto ptr1 = params[0].i32();
    auto ptr2 = params[1].i32();
    const std::vector<std::basic_string<char>> args;
    //wasi.argv();
    auto mem = std::get<Memory>(*caller.get_export("memory"));
    //int32_t *ptr3 = ptr1;
    //Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples(ptr3);
    results[0] = 0;
    return std::monostate();
});
}

std::string WASMRuntime::parseWASMFile(const char* fileName) {
std::ifstream watFile;
watFile.open(fileName);
std::stringstream strStream;
strStream << watFile.rdbuf();
return strStream.str();
}

}// namespace NES::Nautilus::Backends::WASM