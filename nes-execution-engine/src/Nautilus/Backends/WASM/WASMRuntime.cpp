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
}

void WASMRuntime::setup() {
}

void WASMRuntime::run(size_t binaryLength, char *queryBinary) {
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

}

std::string WASMRuntime::parseWASMFile(const char* fileName) {
    std::ifstream watFile;
    watFile.open(fileName);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    return strStream.str();
}

}// namespace NES::Nautilus::Backends::WASM