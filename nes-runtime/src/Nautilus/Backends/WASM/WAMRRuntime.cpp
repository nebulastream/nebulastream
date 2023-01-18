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
#ifndef NES_ENABLE_EXPERIMENTAL_EXECUTION_WAMR
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Nautilus/Backends/WASM/WAMRRuntime.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <utility>
#include <wasm_export.h>

namespace NES::Nautilus::Backends::WASM {

WAMRRuntime::WAMRRuntime() {
    //auto pyWat = parseWATFile("/home/victor/wanes-engine/python/python3.11.wat");
}

void WAMRRuntime::setup() {
}

int foo(wasm_exec_env_t exec_env , int a, int b)
{
    auto tmp = exec_env;
    return a+b;
}

int32_t WAMRRuntime::run(size_t binaryLength, char* queryBinary) {

    static NativeSymbol native_symbols[] = {
        EXPORT_WASM_API_WITH_SIG(foo, "(ii)i")
    };

    auto x = binaryLength;
    auto y = queryBinary;
    std::cout << "Starting WAMR\n";
    auto wasmString = parseWATFile("/home/victor/add.wasm");

    auto length = wasmString.length();

    auto wasmBuffer = reinterpret_cast<uint8_t*>(wasmString.data());

    char error_buf[128];
    uint32_t stack_size = 8092, heap_size = 8092;

    if(!wasm_runtime_init()) {
        printf("WAMR init failed\n");
    }
    int n_native_symbols = sizeof(native_symbols) / sizeof(NativeSymbol);
    if (!wasm_runtime_register_natives("env",
                                       native_symbols,
                                       n_native_symbols)) {
        printf("Host function registering failed\n");
    }

    wasm_module_inst_t module_inst;
    wasm_module_t module;

    module = wasm_runtime_load(wasmBuffer, length, error_buf, sizeof(error_buf));
    if (module == nullptr) {
        printf("Loading failed\n");
    } else {
        module_inst = wasm_runtime_instantiate(module, stack_size, heap_size, error_buf, sizeof(error_buf));
        auto func = wasm_runtime_lookup_function(module_inst, "execute", nullptr);
        auto exec_env = wasm_runtime_create_exec_env(module_inst, stack_size);
        wasm_val_t results[1];

        if (wasm_runtime_call_wasm_a(exec_env, func, 1, results, 0, nullptr)) {
            printf("add function return: %d\n", results[0].of.i32);
        } else {
            printf("%s\n", wasm_runtime_get_exception(module_inst));
        }
    }
    return 0;
}

void WAMRRuntime::linkHostFunction(const std::string& proxyFunctionName) {
    if (proxyFunctionName == "NES__Runtime__TupleBuffer__getBuffer") {
        host_NES__Runtime__TupleBuffer__getBuffer(proxyFunctionName);
    } else if (proxyFunctionName == "NES__Runtime__TupleBuffer__getBufferSize") {
        host_NES__Runtime__TupleBuffer__getBufferSize(proxyFunctionName);
    } else if (proxyFunctionName == "NES__Runtime__TupleBuffer__getNumberOfTuples") {
        //host_NES__Runtime__TupleBuffer__getNumberOfTuples(proxyFunctionName);
    } else if (proxyFunctionName == "NES__Runtime__TupleBuffer__setNumberOfTuples") {
        //host_NES__Runtime__TupleBuffer__setNumberOfTuples(proxyFunctionName);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void WAMRRuntime::prepareCPython() {
}

void WAMRRuntime::host_NES__Runtime__TupleBuffer__getBuffer(const std::string& proxyFunctionName) {
    std::cout << proxyFunctionName << std::endl;
}

/*uint64_t*/ void WAMRRuntime::host_NES__Runtime__TupleBuffer__getBufferSize(const std::string& proxyFunctionName) {
    std::cout << proxyFunctionName << std::endl;

}

std::string WAMRRuntime::parseWATFile(const char* fileName) {
    std::ifstream watFile(fileName);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    return strStream.str();
}

}// namespace NES::Nautilus::Backends::WASM
#endif