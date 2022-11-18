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
#include <iostream>

namespace NES::Nautilus::Backends::WASM {


void WASMRuntime::setup() {
    engine = wasm_engine_new();
    wasmtime_store_t *store = wasmtime_store_new(engine, nullptr, nullptr);
    wasmtime_context_t *context = wasmtime_store_context(store);
    wasmtime_error_t *error;
    wasm_trap_t *trap = nullptr;

    wasi_config_t *wasiConfig = wasi_config_new();
    initWASI(wasiConfig);
    error = wasmtime_context_set_wasi(context, wasiConfig);
    if (error != nullptr) {
        std::cout << "failed to instantiate WASI" << std::endl;
    }
    wasm_byte_vec_t wasm_bytes, cpythonBytes;
    parseWASMFile(&cpythonBytes, cpythonFilePath);

    wasmtime_module_t* queryModule = nullptr;
    wasmtime_module_t* cpythonModule = nullptr;
    /*
    error = wasmtime_module_new(engine, (uint8_t*) wasm_bytes.data, wasm_bytes.size, &query_module);
    if (error != nullptr) {
        std::cout << "failed to compile wasm module" << std::endl;
    }
    wasm_byte_vec_delete(&wasm_bytes);
    */

    error = wasmtime_module_new(engine, (uint8_t*) cpythonBytes.data, cpythonBytes.size, &cpythonModule);
    if (error != nullptr) {
        std::cout << "failed to compile module" << std::endl;
    }
    wasm_byte_vec_delete(&cpythonBytes);

    wasmtime_linker_t* linker = wasmtime_linker_new(engine);
    error = wasmtime_linker_define_wasi(linker);
    if (error != nullptr) {
        std::cout << "failed to link WASI" << std::endl;
    }
    wasmtime_instance_t cpythonInstance;
    error = wasmtime_linker_instantiate(linker, context, cpythonModule, &cpythonInstance, &trap);
    if (error != nullptr || trap != nullptr) {
        std::cout << "failed to instantiate cpython"<< std::endl;
    }

    error = wasmtime_linker_define_instance(linker, context, "cpython", strlen("cpython"), &cpythonInstance);
    if (error != nullptr) {
        std::cout << "failed to link cpython_instance" << std::endl;
    }
    /*
    wasmtime_instance_t wasm_instance;
    error = wasmtime_linker_instantiate(linker, context, wasm_module, &wasm_instance, &trap);
    if (error != NULL || trap != NULL)
        exit_with_error("failed to instantiate wasm_instance", error, trap);
    */
}

void WASMRuntime::initWASI(wasi_config_t* wasiConfig) {
    //TODO: wasi_config_set_argv(wasiConfig, 1, args);
    wasi_config_inherit_argv(wasiConfig);
    wasi_config_inherit_env(wasiConfig);
    wasi_config_inherit_stdin(wasiConfig);
    wasi_config_inherit_stdout(wasiConfig);
    wasi_config_inherit_stderr(wasiConfig);
    //TODO: replace path
    wasi_config_preopen_dir(wasiConfig, "/home/victor/wanes-engine/python", ".");
}

void WASMRuntime::parseWASMFile(wasm_byte_vec_t* bytes, const char* filename) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        printf("> Error loading file!\n");
    }
    fseek(file, 0L, SEEK_END);
    size_t fileSize = ftell(file);
    wasm_byte_vec_new_uninitialized(bytes, fileSize);
    fseek(file, 0L, SEEK_SET);
    if (fread(bytes->data, fileSize, 1, file) != 1) {
        printf("> Error loading module!\n");
    }
    fclose(file);
}

}// namespace NES::Nautilus::Backends::WASM