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
#include <wasm_export.h>
#include <Runtime/WorkerContext.hpp>
#include "Runtime/BufferManager.hpp"

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

int foo(wasm_exec_env_t execEnv, int a, int b) {
    std::cout << "Calling foo\n";
    auto tmp = execEnv;
    return a + b;
}

class DummyClass {
  public:
    DummyClass(int* p, uint8_t s) : ptr(p), size(s) {}
    int* getptr() {
        return ptr;
    }
    [[nodiscard]] uint32_t getSize() const {
        return size;
    }
    void setSize(uint32_t size2) {
        size = size2;
    }
    void setPtr(int* p) {
        ptr = p;
    }
  private:
    int* ptr = nullptr;
    uint32_t size = 10;
};

void dataRead(wasm_exec_env_t execEnv, uint32_t msg_offset, int32_t buf_len) {
    std::cout << "Received: " << msg_offset << " and " << buf_len << std::endl;
    wasm_module_inst_t moduleInstance = get_module_inst(execEnv);
    const char* buffer = "7";
    char* msg;

    if (!wasm_runtime_validate_app_str_addr(moduleInstance, msg_offset))
        return;

    msg = static_cast<char*>(wasm_runtime_addr_app_to_native(moduleInstance, msg_offset));
    std::cout << msg << std::endl;
    strncpy(msg, buffer, 1);
    std::cout << msg << std::endl;
}

void writeClass(wasm_exec_env_t execEnv, uint32_t memoryOffset) {
    std::cout << "Received: " << memoryOffset << std::endl;
    wasm_module_inst_t moduleInstance = get_module_inst(execEnv);
    DummyClass Dummy{nullptr, 1};
    auto *dummyPtr = &Dummy;
    dummyPtr->setSize(4);
    auto *memPtr = static_cast<DummyClass*>(wasm_runtime_addr_app_to_native(moduleInstance, memoryOffset));
    *memPtr = *dummyPtr;
}

void readClass(wasm_exec_env_t execEnv, uint32_t memoryOffset) {
    std::cout << "Received: " << memoryOffset << std::endl;
    wasm_module_inst_t moduleInstance = get_module_inst(execEnv);
    auto *memPtr = static_cast<Runtime::TupleBuffer*>(wasm_runtime_addr_app_to_native(moduleInstance, memoryOffset));
    auto bufferSize = memPtr->getBufferSize();
    std::cout << "Class members(ptr, bufferSize): (" << *memPtr << "," << bufferSize << ")" << std::endl;
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
    auto wasmTBPtr = wasm_runtime_module_dup_data(moduleInstance, reinterpret_cast<const char*>(tupleBuffer), 1024);
    auto wasmBufferPtr = wasm_runtime_module_dup_data(moduleInstance, reinterpret_cast<const char*>(p), tupleBuffer->getBufferSize());
    std::cout << "-> " << tupleBuffer->getBufferSize() << std::endl;
    auto *memPtr = static_cast<Runtime::TupleBuffer*>(wasm_runtime_addr_app_to_native(moduleInstance, wasmTBPtr));
    memPtr->setBufferSize(1024);
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

uint32_t host_allocateBufferProxy(wasm_exec_env_t execEnv, uint32_t pointer) {
    wasm_module_inst_t moduleInstance = get_module_inst(execEnv);
    auto *memPtr = wasm_runtime_addr_app_to_native(moduleInstance, pointer);
    auto *tmp = allocateBufferProxy(memPtr);
    auto x = static_cast<Runtime::TupleBuffer*>(tmp);
    auto y = *x;
    auto wasmPtr = wasm_runtime_module_malloc(moduleInstance, sizeof(y), &tmp);
    return wasmPtr;
}

uint64_t host_getWatermark(wasm_exec_env_t execEnv, uint32_t pointer) {
    wasm_module_inst_t moduleInstance = get_module_inst(execEnv);
    auto *memPtr = wasm_runtime_addr_app_to_native(moduleInstance, pointer);
    auto *buffer = static_cast<Runtime::TupleBuffer*>(memPtr);
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) memPtr;
    auto wm = thisPtr_->getWatermark();
    std::cout << "Watermark: " << wm << std::endl;
    if (wm == 20) {
        thisPtr_->release();
    }
    return wm;
}

void host_setWatermark(wasm_exec_env_t execEnv, uint32_t pointer, uint64_t value) {
    wasm_module_inst_t moduleInstance = get_module_inst(execEnv);
    auto *memPtr = wasm_runtime_addr_app_to_native(moduleInstance, pointer);
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) memPtr;
    thisPtr_->setWatermark(value);
}

int32_t WAMRRuntime::run(size_t binaryLength, char* queryBinary) {
    std::cout << "Starting WAMR\n";
    static NativeSymbol nativeSymbols[] = {
        EXPORT_WASM_API_WITH_SIG(foo, "(ii)i"),
        EXPORT_WASM_API_WITH_SIG(dataRead, "(ii)"),
        EXPORT_WASM_API_WITH_SIG(writeClass, "(i)"),
        EXPORT_WASM_API_WITH_SIG(readClass, "(i)"),
        EXPORT_WASM_API_WITH_SIG(host_allocateBufferProxy, "(i)i"),
        EXPORT_WASM_API_WITH_SIG(allocaBuffer, "()i"),
        EXPORT_WASM_API_WITH_SIG(host_getWatermark, "(i)I"),
        EXPORT_WASM_API_WITH_SIG(host_setWatermark, "(iI)")
    };

    auto x = binaryLength;
    auto y = queryBinary;
    auto wasmString = parseWATFile("/home/victor/add.wasm");

    auto length = wasmString.length();
    auto wasmBuffer = reinterpret_cast<uint8_t*>(wasmString.data());

    char errorBuffer[128];
    uint32_t stackSize = 4*8092, heapSize = 16*8092;

    if(!wasm_runtime_init()) {
        printf("WAMR init failed\n");
    }
    int numNativeSymbols = sizeof(nativeSymbols) / sizeof(NativeSymbol);
    if (!wasm_runtime_register_natives("ProxyFunctions",
                                       nativeSymbols,
                                       numNativeSymbols)) {
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
