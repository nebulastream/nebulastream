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

#include <Nautilus/Backends/Experimental/Vectorization/CUDA/CUDAKernelCompiler.hpp>

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
#include <Util/Timer.hpp>

namespace NES::Nautilus::Backends::CUDA {

CUDAKernelCompiler::CUDAKernelCompiler(std::string kernelWrapperName)
    : kernelWrapperName(std::move(kernelWrapperName))
    , kernelName("cudaKernel")
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

    auto schemaSize = Nautilus::Value<Nautilus::UInt64>((uint64_t)0);
    schemaSize.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 3, NES::Nautilus::IR::Types::StampFactory::createUInt64Stamp());

    auto vectorizedOperator = std::dynamic_pointer_cast<Runtime::Execution::Operators::VectorizableOperator>(nautilusOperator);

    auto trace = Nautilus::Tracing::traceFunction([&]() {
        auto traceContext = Nautilus::Tracing::TraceContext::get();
        traceContext->addTraceArgument(recordBuffer.getReference().ref);
        traceContext->addTraceArgument(schemaSize.ref);
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

std::shared_ptr<CodeGen::CodeGenerator> CUDAKernelCompiler::getCudaKernelWrapper(const IR::BasicBlockPtr& functionBasicBlock) {
    std::vector<std::string> specifiers{"extern \"C\""};
    std::vector<std::string> arguments;
    for (auto i = 0ull; i < functionBasicBlock->getArguments().size(); i++) {
        auto argument = functionBasicBlock->getArguments()[i];
        auto var = CUDALoweringInterface::getVariable(argument->getIdentifier());
        auto type = CUDALoweringInterface::getType(argument->getStamp());
        arguments.emplace_back(type + " " + var);
    }

    auto kernelWrapperFn = std::make_shared<CodeGen::CPP::Function>(specifiers, "auto", kernelWrapperName, arguments);
    auto kernelWrapperFunctionBody = std::make_shared<CodeGen::Segment>();
    auto inputBufferArg = functionBasicBlock->getArguments().at(0);
    auto inputBufferVar = CUDALoweringInterface::getVariable(inputBufferArg->getIdentifier());
    auto inputBufferType = CUDALoweringInterface::getType(inputBufferArg->getStamp());
    auto numberOfTuples = "number_of_tuples";
    auto tupleBuffer = "tuple_buffer";
    auto inputBufferSize = "input_buffer_size";
    auto threadsPerBlock = "threads_per_block";
    auto blocksPerGrid = "blocks_per_grid";
    auto deviceInputBuffer = "device_input_buffer";
    auto getNumberOfTuples = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__getNumberOfTuples = (uint64_t(*)(void*)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples)));
    auto numberOfTuplesStmt = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto {} = NES__Runtime__TupleBuffer__getNumberOfTuples({})", numberOfTuples, inputBufferVar));
    auto getBuffer = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto NES__Runtime__TupleBuffer__getBuffer = (void*(*)(void*)) {}", fmt::ptr(NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getBuffer)));
    auto tupleBufferStmt = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto {} = NES__Runtime__TupleBuffer__getBuffer({})", tupleBuffer, inputBufferVar));
    auto inputBufferSizeStmt = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto {} = {} * sizeof(uint8_t)", inputBufferSize, numberOfTuples));
    auto deviceInputBufferDecl = std::make_shared<CodeGen::CPP::Statement>(fmt::format("{} {}", inputBufferType, deviceInputBuffer));
    auto cudaMallocCall = std::make_shared<CodeGen::CPP::Statement>(fmt::format("cudaMalloc(&{}, {})", deviceInputBuffer, inputBufferSize));
    auto cudaMemcpyHtoDCall = std::make_shared<CodeGen::CPP::Statement>(fmt::format("cudaMemcpy({}, {}, {}, cudaMemcpyHostToDevice)", deviceInputBuffer, tupleBuffer, inputBufferSize));
    // TODO This should be a parameter for performance tuning.
    auto threadsPerBlockStmt = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto {} = 32", threadsPerBlock));
    auto blocksPerGridStmt = std::make_shared<CodeGen::CPP::Statement>(fmt::format("auto {} = ({} + {} - 1) / {}", blocksPerGrid, numberOfTuples, threadsPerBlock, threadsPerBlock));
    auto printCall = std::make_shared<CodeGen::CPP::Statement>(fmt::format("std::cout << \"Number of tuples, threads per block, blocks per grid: \" << {} << \", \" << {} << \", \" << {} << std::endl", numberOfTuples, threadsPerBlock, blocksPerGrid));
    auto kernelCall = std::make_shared<CodeGen::CPP::Statement>(fmt::format("{}<<<{}, {}>>>({})", kernelName, blocksPerGrid, threadsPerBlock, deviceInputBuffer));
    auto cudaMemcpyDtoHCall = std::make_shared<CodeGen::CPP::Statement>(fmt::format("cudaMemcpy({}, {}, {}, cudaMemcpyDeviceToHost)", tupleBuffer, deviceInputBuffer, inputBufferSize));
    auto cudaFreeCall = std::make_shared<CodeGen::CPP::Statement>(fmt::format("cudaFree({})", deviceInputBuffer));
    kernelWrapperFunctionBody->add(getNumberOfTuples);
    kernelWrapperFunctionBody->add(numberOfTuplesStmt);
    kernelWrapperFunctionBody->add(getBuffer);
    kernelWrapperFunctionBody->add(tupleBufferStmt);
    kernelWrapperFunctionBody->add(inputBufferSizeStmt);
    kernelWrapperFunctionBody->add(deviceInputBufferDecl);
    kernelWrapperFunctionBody->add(cudaMallocCall);
    kernelWrapperFunctionBody->add(cudaMemcpyHtoDCall);
    kernelWrapperFunctionBody->add(threadsPerBlockStmt);
    kernelWrapperFunctionBody->add(blocksPerGridStmt);
    kernelWrapperFunctionBody->add(printCall);
    kernelWrapperFunctionBody->add(kernelCall);
    kernelWrapperFunctionBody->add(cudaMemcpyDtoHCall);
    kernelWrapperFunctionBody->add(cudaFreeCall);
    kernelWrapperFn->addSegment(kernelWrapperFunctionBody);
    return kernelWrapperFn;
}

std::unique_ptr<CodeGen::CodeGenerator> CUDAKernelCompiler::createCodeGenerator(const std::shared_ptr<IR::IRGraph>& irGraph) {
    auto cudaLoweringPlugin = CUDALoweringInterface();

    auto codeGen = std::make_unique<CodeGen::CPP::CPPCodeGenerator>();
    codeGen->addInclude("<cstdint>");
    codeGen->addInclude("<iostream>");

    auto functionOperation = irGraph->getRootOperation();
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    auto kernelWrapperFn = std::dynamic_pointer_cast<CodeGen::CPP::Function>(getCudaKernelWrapper(functionBasicBlock));
    codeGen->addFunction(kernelWrapperFn);

    auto kernelFunctionBody = cudaLoweringPlugin.lowerGraph(irGraph);
    std::vector<std::string> specifiers{"__global__"};
    auto inputBufferArg = functionBasicBlock->getArguments().at(0);
    auto inputBufferVar = CUDALoweringInterface::getVariable(inputBufferArg->getIdentifier());
    auto inputBufferType = CUDALoweringInterface::getType(inputBufferArg->getStamp());
    std::vector<std::string> kernelArguments{inputBufferType + " " + inputBufferVar};
    auto kernelFn = std::make_shared<CodeGen::CPP::Function>(specifiers, "void", kernelName, kernelArguments);
    kernelFn->addSegment(std::move(kernelFunctionBody));
    codeGen->addFunction(kernelFn);

    return std::move(codeGen);
}

std::unique_ptr<KernelExecutable> CUDAKernelCompiler::createExecutable(std::unique_ptr<CodeGen::CodeGenerator> codeGenerator, const CompilationOptions& options, const DumpHelper& dumpHelper) {
    auto timer = Timer<>("CUDAKernelCompilation");
    timer.start();

    auto code = codeGenerator->toString();
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

    return std::make_unique<KernelExecutable>(res.getDynamicObject(), kernelWrapperName);
}

} // namespace NES::Nautilus::Backends::CUDA
