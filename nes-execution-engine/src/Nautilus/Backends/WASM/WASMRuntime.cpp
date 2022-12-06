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
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>

namespace NES::Nautilus::Backends::WASM {

WASMRuntime::WASMRuntime() {
    /*
    engine = wasm_engine_new();
    store = wasmtime_store_new(engine, nullptr, nullptr);
    context = wasmtime_store_context(store);
    trap = nullptr;
    wasiConfig = wasi_config_new();
     */
}

void WASMRuntime::setup() {
    /*
    initWASI();
    error = wasmtime_context_set_wasi(context, wasiConfig);

    linker = wasmtime_linker_new(engine);
    error = wasmtime_linker_define_wasi(linker);
    //exit_with_error("failed to link wasi", error, NULL);

    prepareCPython();
     */
}

void WASMRuntime::run(size_t binaryLength, char *queryBinary) {
    /*
    const char *args[1];
    args[0] = new char[50];
    args[0] = "/home/victor/wanes-engine/python/h.py";
    wasi_config_inherit_env(wasiConfig);
    wasi_config_inherit_stdin(wasiConfig);
    wasi_config_inherit_stdout(wasiConfig);
    wasi_config_inherit_stderr(wasiConfig);
    wasi_config_preopen_dir(wasiConfig, "/home/victor/wanes-engine/python", ".");
    wasi_config_set_argv(wasiConfig, 1, args);
    wasmtime_context_set_wasi(context, wasiConfig);

    linker = wasmtime_linker_new(engine);
    wasmtime_linker_define_wasi(linker);

    prepareCPython();
    wasm_byte_vec_t wasm;
    wasm_byte_vec_new(&wasm, binaryLength, queryBinary);

    // Compile and instantiate our module
    wasmtime_module_t *module = nullptr;
    wasmtime_module_new(engine, (uint8_t*) wasm.data, wasm.size, &module);
    wasm_byte_vec_delete(&wasm);

    trap = nullptr;
    wasmtime_instance_t instance;
    wasmtime_instance_new(context, module, nullptr, 0, &instance, &trap);

    wasmtime_extern_t executeFunction;
    bool ok = wasmtime_instance_export_get(context, &instance, queryEntryName, strlen(queryEntryName), &executeFunction);
    assert(ok);
    assert(executeFunction.kind == WASMTIME_EXTERN_FUNC);

    int a = 6;
    wasmtime_val_t params[1];
    params[0].kind = WASMTIME_I32;
    params[0].of.i32 = a;
    wasmtime_val_t results[1];
    wasmtime_func_call(context, &executeFunction.of.func, params, 1, results, 1, &trap);
    assert(results[0].kind == WASMTIME_I32);

    printf("execute(%d) = %d\n", a, results[0].of.i32);

    wasmtime_module_delete(module);
    //wasmtime_store_delete(store);
    //wasm_engine_delete(engine);
     */

    /*
    wasmtime::Engine engine2;
    wasm_engine_t *engine = wasm_engine_new();
    assert(engine != nullptr);
    // WASM Store
    wasmtime_store_t *store = wasmtime_store_new(engine, nullptr, nullptr);
    assert(store != nullptr);
    // WASM Context
    wasmtime_context_t *context = wasmtime_store_context(store);
    std::cout << "Current path: " << std::filesystem::current_path() << std::endl;
    // Configure WASI and store it within our `wasmtime_store_t`
    wasi_config_t *wasi_config = wasi_config_new();
    assert(wasi_config);
    const char *x[4];
    x[0] = "x";
    x[1] = "add.py";
    x[2] = "3";
    x[3] = "7";
    //wasi_config_set_argv(wasi_config, 2, x);

    const char *names[2];
    const char *values[2];
    names[0] = "PYTHONHOME";
    values[0] = "/";
    names[1] = "PYTHONPATH";
    values[1] = "/home/victor/wanes-engine/python";
    wasi_config_set_env(wasi_config, 2, names, values);
    wasi_config_inherit_stdin(wasi_config);
    wasi_config_inherit_stdout(wasi_config);
    wasi_config_inherit_stderr(wasi_config);
    wasi_config_preopen_dir(wasi_config, "/home/victor/wanes-engine/python", ".");
    wasm_trap_t *trap = nullptr;
    wasmtime_context_set_wasi(context, wasi_config);

    wasm_byte_vec_t wasm_bytes, cpythonBytes;
    wasm_byte_vec_new(&wasm_bytes, binaryLength, queryBinary);
    parseWASMFile(&cpythonBytes, cpythonFilePath);

    wasmtime_module_t *wasm_module = nullptr;
    wasmtime_module_t *cpython_module = nullptr;
    wasmtime_module_new(engine, (uint8_t*) wasm_bytes.data, wasm_bytes.size, &wasm_module);

    wasm_byte_vec_delete(&wasm_bytes);
    wasmtime_module_new(engine, (uint8_t*) cpythonBytes.data, cpythonBytes.size, &cpython_module);

    wasm_byte_vec_delete(&cpythonBytes);

    wasm_functype_t *hello_ty = wasm_functype_new_0_0();
    wasmtime_func_t hello;
    wasmtime_func_new(context, hello_ty, pythonCallback, nullptr, nullptr, &hello);

    wasmtime_instance_t instance;
    wasmtime_extern_t import;
    import.kind = WASMTIME_EXTERN_FUNC;
    import.of.func = hello;
    wasmtime_instance_new(context, wasm_module, &import, 1, &instance, &trap);

    // Create our linker which will be linking our modules together, and then add
    // our WASI instance to it.
    wasmtime_linker_t *linker = wasmtime_linker_new(engine);
    wasmtime_error_t *error = wasmtime_linker_define_wasi(linker);
    if (error != nullptr) {
        std::cout << "ERROR" << std::endl;
    }

    wasmtime_instance_t cpython_instance;
    wasmtime_linker_instantiate(linker, context, cpython_module, &cpython_instance, &trap);


    wasmtime_linker_define_instance(linker, context, "cpython", strlen("cpython"), &cpython_instance);

    wasmtime_instance_t wasm_instance;
    wasmtime_linker_instantiate(linker, context, wasm_module, &wasm_instance, &trap);

    // Lookup our `run` export function
    wasmtime_extern_t run;
    bool ok = wasmtime_instance_export_get(context, &wasm_instance, queryEntryName, strlen(queryEntryName), &run);
    assert(ok);
    assert(run.kind == WASMTIME_EXTERN_FUNC);

    int a = 6;
    wasmtime_val_t params[1];
    params[0].kind = WASMTIME_I32;
    params[0].of.i32 = a;
    wasmtime_val_t results[1];
    wasmtime_func_call(context, &run.of.func, params, 1, results, 1, &trap);
    assert(results[0].kind == WASMTIME_I32);
    printf("execute(%d) = %d\n", a, results[0].of.i32);

    */
    wasmtime::Engine engine;
    wasmtime::Span query{(uint8_t *) queryBinary, binaryLength};
    wasmtime::Store store(engine);

    auto module = wasmtime::Module::compile(engine, query).unwrap();

    wasmtime::Func host_func1 = wasmtime::Func::wrap(store, []() {
        std::cout << "Calling back before RETURN...\n";
    });
    wasmtime::Func host_func2 = wasmtime::Func::wrap(store, []() {
        std::cout << "Calling back before IF...\n";
    });

    auto instance = wasmtime::Instance::create(store, module, {host_func1, host_func2}).unwrap();
    auto execute = std::get<wasmtime::Func>(*instance.get(store, "execute"));

    execute.call(store, {5}).unwrap();
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
    std::cout << "BOOM " << cpythonFilePath << std::endl;
    wasm_byte_vec_delete(&cpythonBytes);
    wasmtime_instance_t cpythonInstance;
    error = wasmtime_linker_define_instance(linker, context, "cpython", strlen("cpython"), &cpythonInstance);
    if (error != nullptr) {
        std::cout << "failed to link cpython" << std::endl;
    }
    error = wasmtime_linker_instantiate(linker, context, cpythonModule, &cpythonInstance, &trap);
    if (error != nullptr || trap != nullptr) {
        std::cout << "failed to instantiate cpython" << std::endl;
    }
     */
}

std::string WASMRuntime::parseWASMFile(const char* fileName) {
    std::ifstream watFile;
    watFile.open(fileName);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    return strStream.str();
}

}// namespace NES::Nautilus::Backends::WASM