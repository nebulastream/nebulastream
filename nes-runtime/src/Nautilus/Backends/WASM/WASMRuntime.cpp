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
#include <Runtime/WorkerContext.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Nautilus/Backends/WASM/WASMRuntime.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <utility>
#include <span>


using namespace wasmtime;

namespace NES::Nautilus::Backends::WASM {

/*
 * ------------- Selected Proxy Functions copied for ease of use
 * ------------- TODO: Delete later!
 */

Runtime::TupleBuffer* allocateBufferProxy(void* workerContextPtr) {
    if (workerContextPtr == nullptr) {
        NES_THROW_RUNTIME_ERROR("worker context should not be null");
    }
    auto wkrCtx = static_cast<Runtime::WorkerContext*>(workerContextPtr);
    auto buffer = wkrCtx->allocateTupleBuffer();
    auto* tb = new Runtime::TupleBuffer(buffer);
    return tb;
}

void emitBufferProxy(void* wc, void* pc, void* tupleBuffer) {
    auto* tb = (Runtime::TupleBuffer*) tupleBuffer;
    auto pipelineCtx = static_cast<Runtime::Execution::PipelineExecutionContext*>(pc);
    auto workerCtx = static_cast<Runtime::WorkerContext*>(wc);
    // check if buffer has values
    if (tb->getNumberOfTuples() != 0) {
        pipelineCtx->emitBuffer(*tb, *workerCtx);
    }
    // delete tuple buffer as it was allocated within the pipeline and is not required anymore
    delete tb;
}

void WASMRuntime::setup() {
    config.wasm_memory64(true);
    config.debug_info(true);
    engine = std::make_shared<Engine>(std::move(config));
    linker = std::make_shared<Linker>(*engine);
    store = std::make_shared<Store>(*engine);
    //auto pyWat = parseWATFile("/home/victor/wanes-engine/python/python3.11.wat");
    //pyModule = std::make_shared<Module>(Module::compile(*engine, pyWat).unwrap());

    auto isPreOpened = wasiConfig->preopen_dir("/home/victor/wanes-engine/python", ".");
    if (!isPreOpened) {
        NES_ERROR("Could not pre-open directory for python")
    }
    wasiConfig->inherit_env();
    wasiConfig->inherit_stdin();
    wasiConfig->inherit_stdout();
    wasiConfig->inherit_stderr();
    store->context().set_wasi(std::move(*wasiConfig)).unwrap();
    linker->define_wasi().unwrap();

    allocateBufferProxy();
    host_NES__Runtime__TupleBuffer__getBuffer();
    host_NES__Runtime__TupleBuffer__getBufferSize();
    host_NES__Runtime__TupleBuffer__getNumberOfTuples();
    host_NES__Runtime__TupleBuffer__setNumberOfTuples();
    host_emitBufferProxy();

    //prepareCPython();
    auto handCodedWat = parseWATFile("/home/victor/scanEmit.wat");
    Span query{(uint8_t*) context->getQueryBinary(), context->getBinaryLength()};
    //auto module = Module::compile(*engine, query).unwrap();
    auto module = Module::compile(*engine, handCodedWat).unwrap();
    auto instance = linker->instantiate(*store, module).unwrap();
    execute = std::make_shared<Func>(std::get<Func>(*instance.get(*store, functionName)));
}

int32_t WASMRuntime::run() {
    ExternRef externref1(context->pipelineExeContext);
    ExternRef externref2(context->workerContext);
    ExternRef externref3(context->tupleBuffer);
    std::vector<Val> params{ externref1, externref2, externref3 };
    //std::vector<Val> params{ (int64_t)1, (int64_t)2, (int64_t)3 };
    auto results = execute->call(*store, params).unwrap();
    if (!results.empty()) {
        auto res = *results[0].externref();
        std::any &data = res.data();
        auto tb = std::any_cast<Runtime::TupleBuffer*>(data);
        tb->release();
    }
    if (!tupleBuffers.empty()) {
        for (auto tb  : tupleBuffers) {
            tb->release();
        }
    }
    return 0;
}

void WASMRuntime::close() {
    if (store != nullptr) {
        store->context().gc();
    }
}

void WASMRuntime::allocateBufferProxy() {
    auto res = linker->func_new(proxyFunctionModule, "allocateBufferProxy", FuncType({ValKind::ExternRef}, {ValKind::ExternRef}),
                                [&](Caller caller, Span<const Val> params, Span<Val> results) {
                                    ExternRef er = *params[0].externref();
                                    std::any &data = er.data();
                                    auto wctx = std::any_cast<std::shared_ptr<Runtime::WorkerContext>>(data);

                                    auto mem = std::get<Memory>(*caller.get_export("memory"));

                                    auto tbuffer = wctx->allocateTupleBuffer();
                                    auto tb = std::make_shared<Runtime::TupleBuffer>(tbuffer);
                                    //auto* tb = new Runtime::TupleBuffer(tbuffer);

                                    //auto tb = Nautilus::Backends::WASM::allocateBufferProxy(wctx);
                                    tupleBuffers.emplace_back(tb);

                                    auto buffer = tb->getBuffer();
                                    for (uint64_t i = 0; i < tb->getBufferSize(); ++i) {
                                        mem.data(*store)[i] = buffer[i];
                                    }
                                    ExternRef res(tb);
                                    results[0] = res;
                                    return std::monostate();
                                });
}

void WASMRuntime::host_NES__Runtime__TupleBuffer__getBuffer() {
    auto res = linker->func_new(proxyFunctionModule,
                                "NES__Runtime__TupleBuffer__getBuffer",
                                FuncType({ValKind::ExternRef}, {ValKind::I64}),
                                [](auto caller, auto params, auto results) {
                                    ExternRef er = *params[0].externref();
                                    auto mem = std::get<Memory>(*caller.get_export("memory"));
                                    //TODO: Handle multiple Buffer(?)
                                    Val value((int64_t)0);
                                    results[0] = value;
                                    return std::monostate();
                                });
}

void WASMRuntime::host_NES__Runtime__TupleBuffer__getBufferSize() {
    auto res = linker->func_new(proxyFunctionModule,
                                "NES__Runtime__TupleBuffer__getBufferSize",
                                FuncType({ValKind::I32}, {ValKind::I64}),
                                [](auto caller, auto params, auto results) {
                                    (void) caller;
                                    ExternRef er = *params[0].externref();
                                    auto tupleBuffer = std::any_cast<std::shared_ptr<Runtime::TupleBuffer>>(er.data());
                                    auto bufferSize = tupleBuffer->getBufferSize();
                                    Val value((int64_t)bufferSize);
                                    results[0] = value;
                                    return std::monostate();
                                });
}

/*uint64_t*/ void WASMRuntime::host_NES__Runtime__TupleBuffer__getNumberOfTuples() {
    auto res = linker->func_new(proxyFunctionModule,
                                "NES__Runtime__TupleBuffer__getNumberOfTuples",
                                FuncType({ValKind::ExternRef}, {ValKind::I64}),
                                [](auto caller, auto params, auto results) {
                                    (void) caller;
                                    ExternRef er = *params[0].externref();
                                    auto tupleBuffer = std::any_cast<std::shared_ptr<Runtime::TupleBuffer>>(er.data());
                                    auto numTuples = tupleBuffer->getNumberOfTuples();
                                    Val value((int64_t)numTuples);
                                    results[0] = value;
                                    return std::monostate();
                                });
}

void WASMRuntime::host_NES__Runtime__TupleBuffer__setNumberOfTuples() {
    auto res = linker->func_new(proxyFunctionModule,
                                "NES__Runtime__TupleBuffer__setNumberOfTuples",
                                FuncType({ValKind::ExternRef, ValKind::I64}, {}),
                                [](auto caller, auto params, auto results) {
                                    (void) caller;
                                    (void) results;
                                    ExternRef er = *params[0].externref();
                                    auto numTuples = params[1].i64();
                                    auto tupleBuffer = std::any_cast<std::shared_ptr<Runtime::TupleBuffer>>(er.data());
                                    tupleBuffer->setNumberOfTuples((uint64_t)numTuples);
                                    return std::monostate();
                                });
}

void WASMRuntime::host_emitBufferProxy() {
    auto res = linker->func_new(proxyFunctionModule,
                                "emitBufferProxy",
                                FuncType({ValKind::ExternRef, ValKind::ExternRef, ValKind::ExternRef}, {}),
                                [](auto caller, auto params, auto results) {
                                    (void) caller;
                                    (void) results;
                                    ExternRef erWcCtx = *params[0].externref();
                                    ExternRef erPipelineCtx = *params[1].externref();
                                    ExternRef erTBCtx = *params[2].externref();
                                    auto wcCtx = std::any_cast<std::shared_ptr<Runtime::WorkerContext>>(erWcCtx.data());
                                    auto pipelineContext = std::any_cast<std::shared_ptr<Runtime::Execution::PipelineExecutionContext>>(erPipelineCtx.data());
                                    auto tupleBuffer = std::any_cast<std::shared_ptr<Runtime::TupleBuffer>>(erTBCtx.data());
                                    if (tupleBuffer->getNumberOfTuples() != 0) {
                                        pipelineContext->emitBuffer(*tupleBuffer, *wcCtx);
                                    }
                                    //tupleBuffer->release();
                                    return std::monostate();
                                });
}

void WASMRuntime::prepareCPython() {
    /**
     * This host function overwrites the WASI function args_sizes_get which is called by the cpython wasm interpreter.
     * With this, we "trick" cpython into thinking that we have at least 1 function argument.
     */
    auto argsSizesGet = linker->func_new("cpython",
                                        "args_sizes_get",
                                        FuncType({ValKind::I32, ValKind::I32}, {ValKind::I32}),
                                        [](auto caller, auto params, auto results) {
                                            (void) caller;
                                            (void) params;
                                            results[0] = 0;
                                            return std::monostate();
                                        });

    /**
     * Same as for args_sizes_get.
     */
    auto argsGet = linker->func_new("cpython",
                                   "args_get",
                                   FuncType({ValKind::I32, ValKind::I32}, {ValKind::I32}),
                                   [](auto caller, auto params, auto results) {
                                       (void) caller;
                                       (void) params;
                                       results[0] = 0;
                                       return std::monostate();
                                   });

    /**
     * This host function is called before python is invoked. It writes the arguments into a args file, which the python UDF
     * then accesses.
     * For now allow UDFs with two arguments of type i32...
     */
    auto prepareArgs = linker->func_new("cpython",
                                       "prepare_args",
                                       FuncType({ValKind::I32, ValKind::I32}, {}),
                                       [](auto caller, auto params, auto results) {
                                           (void) caller;
                                           (void) results;
                                           auto param0 = params[0].i32();
                                           auto param1 = params[1].i32();
                                           std::ofstream file("/home/victor/wanes-engine/python/args.txt");
                                           if (file.is_open()) {
                                               file << param0 << " " << param1;
                                               file.close();
                                           } else {
                                               NES_ERROR("Python args file could not be opened")
                                           }
                                           return std::monostate();
                                       });
    /**
     * This function actually calls the cpython wasm interpreter and executes a python UDF
     */
    auto prepareCPython = linker->func_new("cpython", "cpython", FuncType({}, {ValKind::I32}), [&](auto caller, auto params, auto results) {
        (void) caller;
        (void) params;
        (void) results;
        auto pyInstance = linker->instantiate(*store, *pyModule).unwrap();
        auto pyExecute = std::get<Func>(*pyInstance.get(*store, "_start"));
        auto funcResult = pyExecute.call(*store, {}).unwrap();
        std::ifstream file("/home/victor/wanes-engine/python/args.txt");
        std::string res;
        if (file.is_open()) {
            getline(file, res);
            results[0] = stoi(res);
            file.close();
        } else {
            NES_ERROR("Python args file could not be opened");
        }
        return std::monostate();
    });
}

std::string WASMRuntime::parseWATFile(const char* fileName) {
    std::ifstream watFile;
    watFile.open(fileName);
    std::stringstream strStream;
    strStream << watFile.rdbuf();
    return strStream.str();
}

}// namespace NES::Nautilus::Backends::WASM
