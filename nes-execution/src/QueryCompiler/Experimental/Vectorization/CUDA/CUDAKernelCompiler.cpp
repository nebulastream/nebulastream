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

#include <QueryCompiler/Experimental/Vectorization/CUDA/CUDAKernelCompiler.hpp>

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CPPCompiler/CUDAPlatform.hpp>
#include <Compiler/SourceCode.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Execution/TupleBufferProxyFunctions.hpp>
#include <Nautilus/Backends/Experimental/Vectorization/CUDA/CUDALoweringInterface.hpp>
#include <Nautilus/CodeGen/CPP/CPPCodeGenerator.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Timer.hpp>

extern void addSliceToSliceStore(void*, uint64_t, uint64_t, uint64_t);

namespace NES::Nautilus::Backends::CUDA {

CUDAKernelCompiler::CUDAKernelCompiler(Descriptor descriptor)
    : descriptor(descriptor)
{

}

std::shared_ptr<Nautilus::Tracing::ExecutionTrace> CUDAKernelCompiler::createTrace(const std::shared_ptr<Runtime::Execution::Operators::Operator>& nautilusOperator) {
    auto pipelineExecutionContextRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) nullptr);
    pipelineExecutionContextRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());

    auto workerContextRef = Nautilus::Value<Nautilus::MemRef>((int8_t*) nullptr);
    workerContextRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());

    auto memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef(0)));
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 2, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto recordBuffer = Runtime::Execution::RecordBuffer(memRef);

    auto vectorizedOperator = std::dynamic_pointer_cast<Runtime::Execution::Operators::VectorizableOperator>(nautilusOperator);

    auto trace = Nautilus::Tracing::traceFunction([&]() {
        auto traceContext = Nautilus::Tracing::TraceContext::get();
        traceContext->addTraceArgument(recordBuffer.getReference().ref);
        auto ctx = Runtime::Execution::ExecutionContext(workerContextRef, pipelineExecutionContextRef);
        vectorizedOperator->execute(ctx, recordBuffer);
    });

    NES_DEBUG("Kernel Execution Trace = {}", trace->toString());

    return trace;
}

std::shared_ptr<IR::IRGraph> CUDAKernelCompiler::createIR(const std::shared_ptr<Nautilus::Tracing::ExecutionTrace>& trace) {
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    auto ssaTrace = ssaCreationPhase.apply(trace);

    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    auto ir = irCreationPhase.apply(ssaTrace);

    NES_DEBUG("Kernel IR = {}", ir->toString());

    return ir;
}

std::shared_ptr<CodeGen::CPP::Function> CUDAKernelCompiler::getCudaKernelWrapper(const IR::BasicBlockPtr& functionBasicBlock) {
    std::vector<std::string> specifiers{"extern \"C\""};
    auto inputBufferArg = functionBasicBlock->getArguments().at(0);
    auto inputBufferType = CUDALoweringInterface::getType(inputBufferArg->getStamp());
    auto inputBufferVar = CUDALoweringInterface::getVariable(inputBufferArg->getIdentifier());
    auto handlerVar = CUDALoweringInterface::getVariable("handler");
    auto workerIdVar = CUDALoweringInterface::getVariable("workerId");
    std::vector<std::string> arguments{
        inputBufferType + " " + inputBufferVar,
        "void* " + handlerVar,
        "uint64_t " + workerIdVar,
    };

    auto wrapperFunctionName = descriptor.wrapperFunctionName;
    auto kernelWrapperFn = std::make_shared<CodeGen::CPP::Function>(specifiers, "auto", wrapperFunctionName, arguments);
    auto kernelWrapperFunctionBody = std::make_shared<CodeGen::Segment>();

    auto getNumberOfTuples = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__getNumberOfTuples = (uint64_t(*)(void*)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples)));
    kernelWrapperFunctionBody->addToProlog(getNumberOfTuples);

    auto setNumberOfTuples = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__setNumberOfTuples = (void(*)(void*,uint64_t)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples)));
    kernelWrapperFunctionBody->addToProlog(setNumberOfTuples);

    auto getBuffer = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__getBuffer = (void*(*)(void*)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer)));
    kernelWrapperFunctionBody->addToProlog(getBuffer);

    auto getCreationTs = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__getCreationTimestampInMS = (uint64_t(*)(void*)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getCreationTimestampInMS)));
    kernelWrapperFunctionBody->addToProlog(getCreationTs);

    auto postKernelSliceHook = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto postKernelSliceHook = (void(*)(void*,uint64_t,uint64_t,uint64_t)) {}", fmt::ptr(addSliceToSliceStore)));
    kernelWrapperFunctionBody->addToProlog(postKernelSliceHook);

    auto inputSchemaSize = descriptor.inputSchemaSize;
    auto threadsPerBlock = descriptor.threadsPerBlock;
    auto sharedMemorySize = descriptor.sharedMemorySize;
    auto kernelFunctionName = descriptor.kernelFunctionName;
    auto numberOfSlices = descriptor.numberOfSlices;
    auto sliceSize = descriptor.sliceSize;

    auto code = fmt::format(R"(
        auto number_of_tuples = NES__Runtime__TupleBuffer__getNumberOfTuples({0});
        auto buffer = NES__Runtime__TupleBuffer__getBuffer({0});
        auto host_buffer = reinterpret_cast<uint8_t*>(buffer);
        auto creation_ts = NES__Runtime__TupleBuffer__getCreationTimestampInMS({0});
        uint8_t* gpu_buffer;
        uint64_t tuple_size = {1};
        auto input_buffer_size = number_of_tuples * tuple_size;
        cudaCheck(cudaMalloc(&gpu_buffer, input_buffer_size));
        cudaCheck(cudaMemcpy(gpu_buffer, host_buffer, input_buffer_size, cudaMemcpyHostToDevice));
        TupleMetadata* gpu_metadata;
        auto metadata_size = number_of_tuples * sizeof(TupleMetadata);
        cudaCheck(cudaMalloc(&gpu_metadata, metadata_size));
        auto host_tuple_buffer = TupleBuffer{{ gpu_buffer, number_of_tuples, tuple_size, creation_ts, gpu_metadata }};

        auto number_of_slices = {2};
        auto slice_size_in_byte = {3};
        auto timestamp_size_in_byte = sizeof(uint64_t);
        auto slice_buffer_size = number_of_slices * (slice_size_in_byte + timestamp_size_in_byte);
        uint8_t* gpu_slice_buffer;
        cudaCheck(cudaMalloc(&gpu_slice_buffer, slice_buffer_size));
        auto host_slice_store = SliceStore{{ gpu_slice_buffer }};

        auto threads_per_block = {4};
        auto blocks_per_grid = (number_of_tuples + threads_per_block - 1) / threads_per_block;
        auto shared_memory_size = {5};
        std::cout << "Number of tuples, blocks per grid: "
                  << number_of_tuples << ", "
                  << blocks_per_grid << std::endl;

        {6}<<<blocks_per_grid, threads_per_block, shared_memory_size>>>(host_tuple_buffer, host_slice_store);

        if (number_of_slices > 0 && postKernelSliceHook != nullptr) {{
            auto slice_buffer = (uint8_t*)malloc(slice_buffer_size);
            memset(slice_buffer, 0, slice_buffer_size);
            cudaCheck(cudaMemcpy(slice_buffer, gpu_slice_buffer, slice_buffer_size, cudaMemcpyDeviceToHost));
            auto handler = {7};
            auto workerId = {8};
            auto aggregate = *reinterpret_cast<uint64_t*>(slice_buffer);
            auto timestamp = *reinterpret_cast<uint64_t*>(slice_buffer + timestamp_size_in_byte);
            postKernelSliceHook(handler, workerId, aggregate, timestamp);
            free(slice_buffer);
        }}

        cudaCheck(cudaMemcpy(host_buffer, gpu_buffer, input_buffer_size, cudaMemcpyDeviceToHost));

        auto host_metadata = (TupleMetadata*)malloc(metadata_size);
        cudaCheck(cudaMemcpy(host_metadata, gpu_metadata, metadata_size, cudaMemcpyDeviceToHost));
        uint64_t new_number_of_tuples = 0;
        for (int i = 0; i < number_of_tuples; i++) {{
            auto metadata = host_metadata[i];
            if (metadata.valid == 1) {{
                auto dst = host_buffer + (tuple_size * new_number_of_tuples);
                auto src = host_buffer + (tuple_size * i);
                memcpy(dst, src, tuple_size);
                new_number_of_tuples++;
            }}
        }}

        NES__Runtime__TupleBuffer__setNumberOfTuples({0}, new_number_of_tuples);

        cudaCheck(cudaFree(gpu_slice_buffer));
        cudaCheck(cudaFree(gpu_metadata));
        cudaCheck(cudaFree(gpu_buffer))
    )", inputBufferVar, inputSchemaSize, numberOfSlices,
        sliceSize, threadsPerBlock, sharedMemorySize, kernelFunctionName,
        handlerVar, workerIdVar);

    auto stmt = std::make_shared<CodeGen::CPP::Statement>(code);
    kernelWrapperFunctionBody->add(stmt);
    kernelWrapperFn->addSegment(kernelWrapperFunctionBody);
    return kernelWrapperFn;
}

std::shared_ptr<CodeGen::CPP::Function> CUDAKernelCompiler::cudaErrorCheck() {
    std::vector<std::string> specifiers{};
    std::vector<std::string> arguments{"cudaError_t code", "const char *file", "int line"};
    auto fn = std::make_shared<CodeGen::CPP::Function>(specifiers, "void", "cudaErrorCheck", arguments);
    auto functionBody = std::make_shared<CodeGen::Segment>();
    auto code = R"(
        if (code != cudaSuccess) {
            auto error_string = cudaGetErrorString(code);
            std::cerr << "Error at " << file << ":" << line << ": " << error_string << std::endl;
        }
    )";
    auto stmt = std::make_shared<CodeGen::CPP::Statement>(code);
    functionBody->add(stmt);
    fn->addSegment(functionBody);
    return fn;
}

std::shared_ptr<CodeGen::CPP::Function> CUDAKernelCompiler::getBuffer() {
    std::vector<std::string> specifiers{"__device__"};
    std::vector<std::string> arguments{"const TupleBuffer tupleBuffer"};
    auto fn = std::make_shared<CodeGen::CPP::Function>(specifiers, "uint8_t*", "NES__CUDA__TupleBuffer__getBuffer", arguments);
    auto functionBody = std::make_shared<CodeGen::Segment>();
    auto returnStmt = std::make_shared<CodeGen::CPP::Statement>("return tupleBuffer.buffer");
    functionBody->add(returnStmt);
    fn->addSegment(functionBody);
    return fn;
}

std::shared_ptr<CodeGen::CPP::Function> CUDAKernelCompiler::getNumberOfTuples() {
    std::vector<std::string> specifiers{"__device__"};
    std::vector<std::string> arguments{"const TupleBuffer tupleBuffer"};
    auto fn = std::make_shared<CodeGen::CPP::Function>(specifiers, "uint64_t", "NES__CUDA__TupleBuffer__getNumberOfTuples", arguments);
    auto functionBody = std::make_shared<CodeGen::Segment>();
    auto returnStmt = std::make_shared<CodeGen::CPP::Statement>("return tupleBuffer.numberOfTuples");
    functionBody->add(returnStmt);
    fn->addSegment(functionBody);
    return fn;
}

std::shared_ptr<CodeGen::CPP::Function> CUDAKernelCompiler::getCreationTs() {
    std::vector<std::string> specifiers{"__device__"};
    std::vector<std::string> arguments{"const TupleBuffer tupleBuffer"};
    auto fn = std::make_shared<CodeGen::CPP::Function>(specifiers, "uint64_t", "NES__CUDA__TupleBuffer_getCreationTimestampInMS", arguments);
    auto functionBody = std::make_shared<CodeGen::Segment>();
    auto stmt = std::make_shared<CodeGen::CPP::Statement>("return tupleBuffer.creationTs");
    functionBody->add(stmt);
    fn->addSegment(functionBody);
    return fn;
}

std::shared_ptr<CodeGen::CPP::Function> CUDAKernelCompiler::getSliceStore() {
    std::vector<std::string> specifiers{"__device__"};
    std::vector<std::string> arguments{"const SliceStore sliceStore"};
    auto fn = std::make_shared<CodeGen::CPP::Function>(specifiers, "uint8_t*", "NES__CUDA__getSliceStore", arguments);
    auto functionBody = std::make_shared<CodeGen::Segment>();
    auto stmt = std::make_shared<CodeGen::CPP::Statement>("return sliceStore.buffer");
    functionBody->add(stmt);
    fn->addSegment(functionBody);
    return fn;
}

std::shared_ptr<CodeGen::CPP::Function> CUDAKernelCompiler::sum() {
    std::vector<std::string> specifiers{"template <typename T, int BLOCK_THREADS, int ITEMS_PER_THREAD>", "__device__"};
    std::vector<std::string> arguments{"const TupleBuffer tupleBuffer", "const uint64_t tid", "const uint64_t offset"};
    auto fn = std::make_shared<CodeGen::CPP::Function>(specifiers, "uint64_t", "NES__CUDA__sum", arguments);
    auto functionBody = std::make_shared<CodeGen::Segment>();
    auto code = R"(
        typedef cub::BlockReduce<T, BLOCK_THREADS> BlockReduceT;
        __shared__ typename BlockReduceT::TempStorage temp_storage;
        auto buffer = tupleBuffer.buffer;
        auto number_of_tuples = tupleBuffer.numberOfTuples;
        auto tuple_size = tupleBuffer.tupleSize;
        T data[ITEMS_PER_THREAD];
        #pragma unroll
        for (int i = 0; i < ITEMS_PER_THREAD; i++) {
            if (tid < number_of_tuples) {
                auto record_offset = (tid * tuple_size) + offset;
                data[i] = *reinterpret_cast<T*>(buffer + record_offset);
            }
        }
        T aggregate = BlockReduceT(temp_storage).Sum(data);
        return aggregate
    )";
    auto stmt = std::make_shared<CodeGen::CPP::Statement>(code);
    functionBody->add(stmt);
    fn->addSegment(functionBody);
    return fn;
}

std::shared_ptr<CodeGen::CPP::Function> CUDAKernelCompiler::setAsValidInMetadata() {
    std::vector<std::string> specifiers{"__device__"};
    std::vector<std::string> arguments{
        "const TupleBuffer tupleBuffer",
        "const uint64_t tid",
    };
    auto fn = std::make_shared<CodeGen::CPP::Function>(specifiers, "void", "NES__CUDA__setAsValidInMetadata", arguments);
    auto functionBody = std::make_shared<CodeGen::Segment>();
    auto code = R"(
        tupleBuffer.metadata[tid].valid = 1;
    )";
    auto stmt = std::make_shared<CodeGen::CPP::Statement>(code);
    functionBody->add(stmt);
    fn->addSegment(functionBody);
    return fn;
}

std::unique_ptr<CodeGen::CodeGenerator> CUDAKernelCompiler::createCodeGenerator(const std::shared_ptr<IR::IRGraph>& irGraph) {
    auto codeGen = std::make_unique<CodeGen::CPP::CPPCodeGenerator>();
    codeGen->addInclude("<cstdint>");
    codeGen->addInclude("<cub/cub.cuh>");

    auto metadata = std::make_shared<CodeGen::CPP::Struct>("TupleMetadata");
    metadata->addField("uint8_t", "valid");
    codeGen->addStruct(metadata);

    auto gpuTupleBuffer = std::make_shared<CodeGen::CPP::Struct>("TupleBuffer");
    gpuTupleBuffer->addField("uint8_t*", "buffer");
    gpuTupleBuffer->addField("uint64_t", "numberOfTuples");
    gpuTupleBuffer->addField("uint64_t", "tupleSize");
    gpuTupleBuffer->addField("uint64_t", "creationTs");
    gpuTupleBuffer->addField("TupleMetadata*", "metadata");
    codeGen->addStruct(gpuTupleBuffer);

    auto sliceStore = std::make_shared<CodeGen::CPP::Struct>("SliceStore");
    sliceStore->addField("uint8_t*", "buffer");
    codeGen->addStruct(sliceStore);

    auto cudaErrorCheckFn = cudaErrorCheck();
    codeGen->addFunction(cudaErrorCheckFn);
    codeGen->addMacro("cudaCheck(err) do { cudaErrorCheck((err), __FILE__, __LINE__); } while (false)");

    auto bufferFn = getBuffer();
    codeGen->addFunction(bufferFn);

    auto numberOfTuplesFn = getNumberOfTuples();
    codeGen->addFunction(numberOfTuplesFn);

    auto creationTsFn = getCreationTs();
    codeGen->addFunction(creationTsFn);

    auto getSliceStoreFn = getSliceStore();
    codeGen->addFunction(getSliceStoreFn);

    auto sumFn = sum();
    codeGen->addFunction(sumFn);

    auto setAsValidInMetadataFn = setAsValidInMetadata();
    codeGen->addFunction(setAsValidInMetadataFn);

    auto functionOperation = irGraph->getRootOperation();
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    auto kernelWrapperFn = getCudaKernelWrapper(functionBasicBlock);
    codeGen->addFunction(kernelWrapperFn);

    std::vector<std::string> specifiers{"__global__"};
    auto tupleBufferArg = functionBasicBlock->getArguments().at(0);
    auto tupleBufferVar = CUDALoweringInterface::getVariable(tupleBufferArg->getIdentifier());
    auto sliceStoreVar = CUDALoweringInterface::getVariable("slice_store");
    std::vector<std::string> kernelArguments{
        fmt::format("const TupleBuffer {}", tupleBufferVar),
        fmt::format("const SliceStore {}", sliceStoreVar)
    };
    auto kernelFn = std::make_shared<CodeGen::CPP::Function>(
        specifiers,
        "void",
        descriptor.kernelFunctionName,
        kernelArguments
    );
    auto frame = CodeGen::IRLoweringInterface::RegisterFrame();
    frame.setValue(CUDALoweringInterface::TUPLE_BUFFER_IDENTIFIER, tupleBufferVar);
    frame.setValue(CUDALoweringInterface::SLICE_STORE_IDENTIFIER, sliceStoreVar);
    auto cudaLoweringPlugin = CUDALoweringInterface(frame);
    auto kernelFunctionBody = cudaLoweringPlugin.lowerGraph(irGraph);
    kernelFn->addSegment(std::move(kernelFunctionBody));
    codeGen->addFunction(kernelFn);

    return std::move(codeGen);
}

std::unique_ptr<KernelExecutable> CUDAKernelCompiler::createExecutable(std::unique_ptr<CodeGen::CodeGenerator> codeGenerator, const CompilationOptions& options) {
    auto timer = Timer<>("CUDAKernelCompilation");
    timer.start();

    auto code = codeGenerator->toString();
    auto dumpHelper = DumpHelper::create(
        options.getIdentifier(),
        options.isDumpToConsole(),
        options.isDumpToFile()
    );
    dumpHelper.dump("code.cu", code);
    timer.snapshot("CodeGeneration");

    auto compiler = Compiler::CPPCompiler::create();
    auto sourceCode = std::make_unique<Compiler::SourceCode>(Compiler::Language::CPP, code);

    std::vector<std::shared_ptr<Compiler::ExternalAPI>> externalApis;
    externalApis.push_back(std::make_shared<Compiler::CUDAPlatform>(options.getCUDASdkPath()));

    auto request = Compiler::CompilationRequest::create(std::move(sourceCode),
                                                        "cudaQuery",
                                                        options.isDebug(),
                                                        false,
                                                        options.isOptimize(),
                                                        options.isDebug(),
                                                        externalApis);
    auto res = compiler->compile(request);
    timer.snapshot("Compilation");

    return std::make_unique<KernelExecutable>(res.getDynamicObject(), descriptor.wrapperFunctionName);
}

} // namespace NES::Nautilus::Backends::CUDA
