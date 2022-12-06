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
#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMRUNTIME_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMRUNTIME_HPP_

#include <wasmtime.hh>
#include <wasm.h>

namespace NES::Nautilus::Backends::WASM {

class WASMRuntime {
  public:
    WASMRuntime();
    void setup();
    void run(size_t binaryLength, char *queryBinary);

  private:
    //wasmtime::Engine engine;
    /*
    wasm_engine_t *engine;
    wasmtime_store_t *store;
    wasmtime_context_t *context;
    wasmtime_error_t *error;
    wasm_trap_t *trap;
    wasi_config_t *wasiConfig;
    */
    const char* cpythonFilePath = "/home/victor/wanes-engine/python/python3.11.wasm";
    //wasmtime_instance_t queryInstance;
    //wasmtime_linker_t *linker;
    const char queryEntryName[8] = "execute";
    //const char cpythonEntryName[7] = "_start";

    std::string parseWASMFile(const char* fileName);
    void prepareCPython();
};

}// namespace NES::Nautilus::Backends::WASM

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMRUNTIME_HPP_