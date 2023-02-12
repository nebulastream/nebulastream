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
#include <Nautilus/Backends/WASM/WASMExecutionContext.hpp>

namespace NES::Nautilus::Backends::WASM {

class WAMRExecutionEngine {
  public:
    WAMRExecutionEngine();
    void setup(const std::shared_ptr<WASMExecutionContext>& context);
    int32_t run();
    /**
     * Gets passed a pointer to a WorkerContext and creates a TupleBuffer. We copy the TupleBuffer and it's buffer into WASM
     * linear memory. Otherwise, WASM's sandbox will not allow us to access the buffer from within WASM.
     * @param execEnv
     * @param pointer
     * @return WASM pointer to the TupleBuffer
     */
    static uint32_t native_allocateBufferProxy(wasm_exec_env_t execEnv, uintptr_t* pointer);
    static void
    native_emitBufferProxy(wasm_exec_env_t execEnv, uintptr_t* workerCtx, uintptr_t* pipelineCtx, uint32_t tupleBuffer);

  private:
    std::string parseWATFile(const char* fileName);
    void prepareCPython();
    void registerNativeSymbols();
    /**
     * Names and signatures of proxy functions supported by the WASM backend
     */
    NativeSymbol nativeSymbols[2];
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

    /**
     * Length of 128 is defined by WAMR
     */
    char errorBuffer[128];
    /**
     * TODO: Move to a context that we pass into to execution engine
     */
    const uint32_t stackSize = 4 * 8092, heapSize = 16 * 8092;
    const char* cpythonFilePath = "/home/victor/wanes-engine/python/python3.11.wasm";
    const char* proxyFunctionModuleName = "ProxyFunction";
    const char* functionName = "execute";
    wasm_module_inst_t moduleInstance;
    wasm_module_t module;
    wasm_function_inst_t func;
    wasm_exec_env_t execEnv;
};

}// namespace NES::Nautilus::Backends::WASM
#endif//NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WAMRRUNTIME_HPP_
