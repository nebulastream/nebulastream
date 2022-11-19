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

WASMRuntime::WASMRuntime() {
}

void WASMRuntime::setup() {
    /*
    engine = wasm_engine_new();
    store = wasmtime_store_new(engine, nullptr, nullptr);
    context = wasmtime_store_context(store);
    error = nullptr;
    trap = nullptr;

    wasi_config_t *wasiConfig = wasi_config_new();
    //initWASI(wasiConfig);

    error = wasmtime_context_set_wasi(context, wasiConfig);
    if (error != nullptr) {
        std::cout << "failed to instantiate WASI" << std::endl;
    }

    linker = wasmtime_linker_new(engine);
    error = wasmtime_linker_define_wasi(linker);
    if (error != nullptr) {
        std::cout << "failed to link WASI" << std::endl;
    }

    prepareCPython();
     */
}

void WASMRuntime::run(size_t binaryLength, char *queryBinary) {
    // Set up our context
    wasm_engine_t *engine = wasm_engine_new();
    assert(engine != NULL);
    wasmtime_store_t *store = wasmtime_store_new(engine, NULL, NULL);
    assert(store != NULL);
    wasmtime_context_t *context = wasmtime_store_context(store);
    wasmtime_error_t *error;

    wasm_byte_vec_t wasm;
    wasm_byte_vec_new(&wasm, binaryLength, queryBinary);

    // Compile and instantiate our module
    wasmtime_module_t *module = NULL;
    error = wasmtime_module_new(engine, (uint8_t*) wasm.data, wasm.size, &module);
    if (module == NULL) {}
        //exit_with_error("failed to compile module", error, NULL);
    wasm_byte_vec_delete(&wasm);

    wasm_trap_t *trap = NULL;
    wasmtime_instance_t instance;
    error = wasmtime_instance_new(context, module, NULL, 0, &instance, &trap);
    if (error != NULL || trap != NULL) {}
        //exit_with_error("failed to instantiate", error, trap);

    // Lookup our `gcd` export function
    wasmtime_extern_t gcd;
    bool ok = wasmtime_instance_export_get(context, &instance, "execute", 7, &gcd);
    assert(ok);
    assert(gcd.kind == WASMTIME_EXTERN_FUNC);

    // And call it!
    int a = 6;
    wasmtime_val_t params[1];
    params[0].kind = WASMTIME_I32;
    params[0].of.i32 = a;
    wasmtime_val_t results[1];
    error = wasmtime_func_call(context, &gcd.of.func, params, 1, results, 1, &trap);
    if (error != NULL || trap != NULL) {}
        //exit_with_error("failed to call gcd", error, trap);
    assert(results[0].kind == WASMTIME_I32);

    printf("loop(%d) = %d\n", a, results[0].of.i32);

    // Clean up (for now)
    wasmtime_module_delete(module);
    wasmtime_store_delete(store);
    wasm_engine_delete(engine);
}

void WASMRuntime::prepareCPython() {
    /*
    wasm_byte_vec_t cpythonBytes;
    parseWASMFile(&cpythonBytes, cpythonFilePath);
    wasmtime_module_t* cpythonModule = nullptr;
    error = wasmtime_module_new(engine, (uint8_t*) cpythonBytes.data, cpythonBytes.size, &cpythonModule);
    if (error != nullptr) {
        std::cout << "failed to compile cpython module" << std::endl;
    }

    wasm_byte_vec_delete(&cpythonBytes);
    std::cout << "YOLO!" << std::endl;
    wasmtime_instance_t cpythonInstance;
    error = wasmtime_linker_define_instance(linker, context, "cpython", strlen("cpython"), &cpythonInstance);
    if (error != nullptr) {
        std::cout << "failed to link cpython" << std::endl;
    }

    error = wasmtime_linker_instantiate(linker, context, cpythonModule, &cpythonInstance, &trap);
    if (error != nullptr || trap != nullptr) {
        std::cout << "failed to instantiate cpython"<< std::endl;
    }
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