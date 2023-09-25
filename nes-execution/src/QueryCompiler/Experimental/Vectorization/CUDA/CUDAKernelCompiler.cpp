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
    std::vector<std::string> arguments{
        inputBufferType + " " + inputBufferVar,
    };

    auto wrapperFunctionName = descriptor.wrapperFunctionName;
    auto kernelWrapperFn = std::make_shared<CodeGen::CPP::Function>(specifiers, "auto", wrapperFunctionName, arguments);
    auto kernelWrapperFunctionBody = std::make_shared<CodeGen::Segment>();

    auto kernelFunctionName = descriptor.kernelFunctionName;
    auto inputSchemaSize = descriptor.inputSchemaSize;
    auto threadsPerBlock = descriptor.threadsPerBlock;

    auto getNumberOfTuples = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__getNumberOfTuples = (uint64_t(*)(void*)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples)));
    kernelWrapperFunctionBody->addToProlog(getNumberOfTuples);

    auto setNumberOfTuples = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__setNumberOfTuples = (void(*)(void*,uint64_t)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__setNumberOfTuples)));
    kernelWrapperFunctionBody->addToProlog(setNumberOfTuples);

    auto getBuffer = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__getBuffer = (void*(*)(void*)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer)));
    kernelWrapperFunctionBody->addToProlog(getBuffer);

    auto code = fmt::format(R"(
        auto number_of_tuples = NES__Runtime__TupleBuffer__getNumberOfTuples({0});
        auto buffer = NES__Runtime__TupleBuffer__getBuffer({0});
        auto host_buffer = reinterpret_cast<uint8_t*>(buffer);
        auto input_buffer_size = number_of_tuples * {1};
        uint8_t* gpu_buffer;
        cudaCheck(cudaMalloc(&gpu_buffer, input_buffer_size));
        cudaCheck(cudaMemcpy(gpu_buffer, host_buffer, input_buffer_size, cudaMemcpyHostToDevice));
        TupleMetadata* gpu_metadata;
        auto metadata_size = number_of_tuples * sizeof(TupleMetadata);
        cudaCheck(cudaMalloc(&gpu_metadata, metadata_size));
        auto host_tuple_buffer = TupleBuffer{{ tuple_buffer, number_of_tuples, gpu_metadata }};

        auto threads_per_block = {2};
        auto blocks_per_grid = (number_of_tuples + threads_per_block - 1) / threads_per_block;
        std::cout << "Number of tuples, threads per block, blocks per grid: " << number_of_tuples << ", " << threads_per_block << ", " << blocks_per_grid << std::endl;

        {3}<<<blocks_per_grid, threads_per_block>>>(host_tuple_buffer);

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

        cudaCheck(cudaFree(gpu_metadata));
        cudaCheck(cudaFree(gpu_buffer))
    )", inputBufferVar, inputSchemaSize, threadsPerBlock, kernelFunctionName);

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
    codeGen->addInclude("<iostream>");

    auto metadata = std::make_shared<CodeGen::CPP::Struct>("TupleMetadata");
    metadata->addField("uint8_t", "valid");
    codeGen->addStruct(metadata);

    auto gpuTupleBuffer = std::make_shared<CodeGen::CPP::Struct>("TupleBuffer");
    gpuTupleBuffer->addField("uint8_t*", "buffer");
    gpuTupleBuffer->addField("uint64_t", "numberOfTuples");
    gpuTupleBuffer->addField("TupleMetadata*", "metadata");
    codeGen->addStruct(gpuTupleBuffer);

    auto cudaErrorCheckFn = cudaErrorCheck();
    codeGen->addFunction(cudaErrorCheckFn);
    codeGen->addMacro("cudaCheck(err) do { cudaErrorCheck((err), __FILE__, __LINE__); } while (false)");

    auto bufferFn = getBuffer();
    codeGen->addFunction(bufferFn);

    auto numberOfTuplesFn = getNumberOfTuples();
    codeGen->addFunction(numberOfTuplesFn);

    auto setAsValidInMetadataFn = setAsValidInMetadata();
    codeGen->addFunction(setAsValidInMetadataFn);

    auto functionOperation = irGraph->getRootOperation();
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    auto kernelWrapperFn = getCudaKernelWrapper(functionBasicBlock);
    codeGen->addFunction(kernelWrapperFn);

    std::vector<std::string> specifiers{"__global__"};
    auto tupleBufferArg = functionBasicBlock->getArguments().at(0);
    auto tupleBufferVar = CUDALoweringInterface::getVariable(tupleBufferArg->getIdentifier());
    std::vector<std::string> kernelArguments{
        fmt::format("const TupleBuffer {}", tupleBufferVar),
    };
    auto kernelFn = std::make_shared<CodeGen::CPP::Function>(
        specifiers,
        "void",
        descriptor.kernelFunctionName,
        kernelArguments
    );
    auto frame = CodeGen::IRLoweringInterface::RegisterFrame();
    frame.setValue(CUDALoweringInterface::TUPLE_BUFFER_IDENTIFIER, tupleBufferVar);
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
