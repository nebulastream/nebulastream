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
#include <Nautilus/Backends/WASM/WAMRRuntime.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <utility>
#include <Runtime/detail/TupleBufferImpl.hpp>

namespace NES::Nautilus::Backends::WASM {

WAMRRuntime::WAMRRuntime() {
    //auto pyWat = parseWATFile("/home/victor/wanes-engine/python/python3.11.wat");
}

void WAMRRuntime::setup() {
}

void* allocateBufferProxy(void* workerContextPtr) {
    if (workerContextPtr == nullptr) {
        NES_THROW_RUNTIME_ERROR("worker context should not be null");
    }
    auto wkrCtx = static_cast<Runtime::WorkerContext*>(workerContextPtr);
    auto buffer = wkrCtx->allocateTupleBuffer();
    auto* tb = new Runtime::TupleBuffer(buffer);
    return tb;
}

uint32_t allocaBuffer(wasm_exec_env_t execEnv) {
    wasm_module_inst_t moduleInstance = get_module_inst(execEnv);
    auto bm = std::make_shared<Runtime::BufferManager>();
    //auto wc = std::make_shared<Runtime::WorkerContext>(0, bm, 1);
    //Runtime::TupleBuffer* tb = nullptr;

    //auto *tupleBuffer = allocateBufferProxy(&wc);
    auto buf = bm->getBufferBlocking();
    Runtime::TupleBuffer* tupleBuffer = &buf;
    auto p = tupleBuffer->getBuffer();
    auto cb = tupleBuffer->getControlBlockPub();
    auto wasmTBPtr = wasm_runtime_module_dup_data(moduleInstance, reinterpret_cast<const char*>(tupleBuffer), 1024);
    auto wasmBufferPtr = wasm_runtime_module_dup_data(moduleInstance, reinterpret_cast<const char*>(p), tupleBuffer->getBufferSize());
    auto wasmCBPtr = wasm_runtime_module_dup_data(moduleInstance, reinterpret_cast<const char*>(cb), sizeof(*cb));

    auto *nativeTBPtr = static_cast<Runtime::TupleBuffer*>(wasm_runtime_addr_app_to_native(moduleInstance, wasmTBPtr));
    auto *nativeBufferPtr = static_cast<unsigned char*>(wasm_runtime_addr_app_to_native(moduleInstance, wasmBufferPtr));
    auto *nativeCBPtr = static_cast<Runtime::detail::BufferControlBlock*>(wasm_runtime_addr_app_to_native(moduleInstance, wasmCBPtr));
    nativeTBPtr->setBuffer(nativeBufferPtr);
    nativeTBPtr->setControlBlock(nativeCBPtr);

    //std::cout << "! " << nativeTBPtr->getNumberOfTuples() << std::endl;
    //auto* p = static_cast<Runtime::TupleBuffer*>(wasm_runtime_addr_app_to_native(moduleInstance, wasmPtr));
    //auto wasmPtr = wasm_runtime_module_malloc(moduleInstance, 250, &tb2);
    /*
    int x = 6;
    int *y = &x;
    DummyClass dummy = DummyClass(y, 10);
    DummyClass* dummyPtr = &dummy;
    auto lel = (void*) dummyPtr;
    //Return wasm pointers
    auto wasmPtr = wasm_runtime_module_dup_data(moduleInstance, reinterpret_cast<const char*>(dummyPtr), sizeof(dummy));
    auto intPtr = wasm_runtime_module_dup_data(moduleInstance, reinterpret_cast<const char*>(y), sizeof x);
    std::cout << "DummyPtr: " << wasmPtr << " ---- intPtr: " << intPtr << std::endl;
    //Get DummyClass from wasm memory
    auto* p = static_cast<DummyClass*>(wasm_runtime_addr_app_to_native(moduleInstance, wasmPtr));
    auto xx = p->getptr();
    std::cout << "TEST: " << *xx << std::endl;
    //Set native pointer for buffer
    p->setPtr(static_cast<int*>(wasm_runtime_addr_app_to_native(moduleInstance, intPtr)));
    //auto *memPtr = wasm_runtime_addr_app_to_native(moduleInstance, 0);
    //auto * memPtr2 = static_cast<Runtime::TupleBuffer*>(memPtr);
    // *memPtr2 = *x;
    //std::memcpy(tb2, x, 250);
    //std::cout << "After copy: " << memPtr->getWatermark() << std::endl;
    //auto *tupleBuffer = allocateBufferProxy(&wc);
    //auto x = *static_cast<Runtime::TupleBuffer*>(tupleBuffer);
    //auto *memPtr = static_cast<Runtime::TupleBuffer*>(wasm_runtime_addr_app_to_native(moduleInstance, 0));
    // *memPtr = *tupleBuffer;
    auto t = dummyPtr->getptr();
    std::cout << *t << std::endl;
    */
    return wasmTBPtr;
}

uintptr_t* native_allocateBufferProxy(wasm_exec_env_t execEnv, uintptr_t* pointer) {
    (void) execEnv;
    auto* ptr = (void*) pointer;
    auto* tmp = allocateBufferProxy(ptr);
    return static_cast<uintptr_t*>(tmp);
}

uintptr_t* NES_Runtime_TupleBuffer_getBuffer(wasm_exec_env_t execEnv, uintptr_t* pointer) {
    (void) execEnv;
    auto* thisPtr_ = (Runtime::TupleBuffer*) pointer;
    return reinterpret_cast<uintptr_t*>(thisPtr_->getBuffer());
}

uint64_t NES_Runtime_TupleBuffer_getWatermark(wasm_exec_env_t execEnv, uintptr_t* pointer) {
    (void) execEnv;
    auto* thisPtr_ = (Runtime::TupleBuffer*) pointer;
    return thisPtr_->getWatermark();
}

void NES_Runtime_TupleBuffer_setWatermark(wasm_exec_env_t execEnv, uintptr_t* pointer, uint64_t value) {
    (void) execEnv;
    auto* thisPtr_ = (Runtime::TupleBuffer*) pointer;
    return thisPtr_->setWatermark(value);
}

uint64_t NES_Runtime_TupleBuffer_getNumberOfTuples(wasm_exec_env_t execEnv, uintptr_t* pointer) {
    (void) execEnv;
    auto* thisPtr_ = (Runtime::TupleBuffer*) pointer;
    return thisPtr_->getNumberOfTuples();
}

void NES_Runtime_TupleBuffer_setNumberOfTuples(wasm_exec_env_t execEnv, uintptr_t* pointer, uint64_t numberOfTuples) {
    (void) execEnv;
    auto* tupleBuffer = (Runtime::TupleBuffer*) pointer;
    tupleBuffer->setNumberOfTuples(numberOfTuples);
}

uint64_t NES_Runtime_TupleBuffer_getBufferSize(wasm_exec_env_t execEnv, uintptr_t* pointer) {
    (void) execEnv;
    auto* thisPtr_ = (Runtime::TupleBuffer*) pointer;
    return thisPtr_->getBufferSize();
}

int32_t WAMRRuntime::run(size_t binaryLength, char* queryBinary) {
    std::cout << "Starting WAMR\n";
    static NativeSymbol nativeSymbols[] = {EXPORT_WASM_API_WITH_SIG(native_allocateBufferProxy, "(r)r"),
                                           EXPORT_WASM_API_WITH_SIG(allocaBuffer, "()i"),
                                           EXPORT_WASM_API_WITH_SIG(NES_Runtime_TupleBuffer_getWatermark, "(r)I"),
                                           EXPORT_WASM_API_WITH_SIG(NES_Runtime_TupleBuffer_setWatermark, "(rI)"),
                                           EXPORT_WASM_API_WITH_SIG(NES_Runtime_TupleBuffer_getBuffer, "(r)r"),
                                           EXPORT_WASM_API_WITH_SIG(NES_Runtime_TupleBuffer_getNumberOfTuples, "(r)I"),
                                           EXPORT_WASM_API_WITH_SIG(NES_Runtime_TupleBuffer_setNumberOfTuples, "(rI)"),
                                           EXPORT_WASM_API_WITH_SIG(NES_Runtime_TupleBuffer_getBufferSize, "(r)I")};

    auto x = binaryLength;
    auto y = queryBinary;
    auto wasmString = parseWATFile("/home/victor/add.wasm");

    auto length = wasmString.length();
    auto wasmBuffer = reinterpret_cast<uint8_t*>(wasmString.data());

    char errorBuffer[128];
    uint32_t stackSize = 4 * 8092, heapSize = 16 * 8092;

    if (!wasm_runtime_init()) {
        printf("WAMR init failed\n");
    }
    int numNativeSymbols = sizeof(nativeSymbols) / sizeof(NativeSymbol);
    if (!wasm_runtime_register_natives("ProxyFunctions", nativeSymbols, numNativeSymbols)) {
        printf("Host function registering failed\n");
    }

    wasm_module_inst_t moduleInstance;
    wasm_module_t module;

    module = wasm_runtime_load(wasmBuffer, length, errorBuffer, sizeof(errorBuffer));
    if (module == nullptr) {
        printf("Loading failed\n");
    } else {
        moduleInstance = wasm_runtime_instantiate(module, stackSize, heapSize, errorBuffer, sizeof(errorBuffer));
        auto func = wasm_runtime_lookup_function(moduleInstance, "execute", nullptr);
        auto execEnv = wasm_runtime_create_exec_env(moduleInstance, stackSize);
        wasm_val_t results[1];
        std::string s = std::to_string(results[0].of.i64);
        if (wasm_runtime_call_wasm_a(execEnv, func, 1, results, 0, nullptr)) {
            printf("add function return: %ld\n", results[0].of.i64);
        } else {
            printf("%s\n", wasm_runtime_get_exception(moduleInstance));
        }
    }
    return 0;
}

void WAMRRuntime::prepareCPython() {
}

std::string WAMRRuntime::parseWATFile(const char* fileName) {
    std::ifstream watFile(fileName);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    return strStream.str();
}

}// namespace NES::Nautilus::Backends::WASM
