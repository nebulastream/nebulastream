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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMRUNTIME_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMRUNTIME_HPP_

#include <binaryen-c.h>
#include <wasmtime.hh>
#include <wasm.h>

namespace NES::Nautilus::Backends::WASM {

class WASMRuntime {
public:
WASMRuntime();
void setup(const std::vector<std::string>& proxyFunctions);
void run(size_t binaryLength, char *queryBinary);

private:
wasmtime::Engine engine;
wasmtime::Linker linker;
wasmtime::Store store;
wasmtime::WasiConfig wasiConfig;
wasmtime::Config config;
wasmtime::Memory pyMemory = wasmtime::Memory(wasmtime_memory());
wasmtime::Func pyExecute = wasmtime::Func(wasmtime_func());

const char* cpythonFilePath = "/home/victor/wanes-engine/python/python3.11.wasm";
const char udfFileName[7] = "udf.py";
std::string proxyFunctionModule = "ProxyFunction";

void linkHostFunction(const std::string& proxyFunction);
std::string parseWATFile(const char* fileName);
wasmtime::Span<uint8_t> parseWASMFile(const char* fileName);
void prepareCPython();
void host_NES__Runtime__TupleBuffer__getBuffer(const std::string& proxyFunctionName);
void host_NES__Runtime__TupleBuffer__getBufferSize(const std::string& proxyFunctionName);
void host_NES__Runtime__TupleBuffer__getNumberOfTuples(const std::string& proxyFunctionName);
void host_NES__Runtime__TupleBuffer__setNumberOfTuples(const std::string& proxyFunctionName);
};

}// namespace NES::Nautilus::Backends::WASM

#endif//NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMRUNTIME_HPP_