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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WAMRRUNTIME_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WAMRRUNTIME_HPP_

#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <cstdint>
#include <string>
#include <wasm_export.h>

namespace NES::Nautilus::Backends::WASM {

class WAMRRuntime {
  public:
    WAMRRuntime();
    void setup();
    int32_t run(size_t binaryLength, char* queryBinary);

  private:

    const char* wat = "(module\n"
                      " (memory $memory 1)\n"
                      " (export \"memory\" (memory $memory))\n"
                      " (export \"execute\" (func $execute))\n"
                      " (func $execute (result i32)\n"
                      "  i32.const 8\n"
                      "  i32.const 6\n"
                      "  i32.add\n"
                      "  return\n"
                      " )\n"
                      ")";

    const char* cpythonFilePath = "/home/victor/wanes-engine/python/python3.11.wasm";
    std::string proxyFunctionModule = "ProxyFunction";

    std::string parseWATFile(const char* fileName);
    void prepareCPython();
};

}// namespace NES::Nautilus::Backends::WASM
#endif//NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WAMRRUNTIME_HPP_
