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

#include "IREECompilerWrapper.hpp"
#include <Util/Logger/Logger.hpp>
#include <iree/compiler/embedding_api.h>
#include <iree/compiler/mlir_interop.h>
#include <llvm/Support/CommandLine.h>
#include <mlir-c/BuiltinAttributes.h>
#include <mlir-c/IR.h>
#include <mlir-c/Support.h>
#include <torch-mlir-c/TorchTypes.h>

namespace NES::Runtime::Execution::Operators
{

using compilerState = struct CompilerState
{
    iree_compiler_session_t *session;
    iree_compiler_source_t *source;
    iree_compiler_output_t *output;
    iree_compiler_invocation_t *inv;
    bool hasCompiled;
};

static void handleCompilerError(iree_compiler_error_t *error)
{
    const char *msg = ireeCompilerErrorGetMessage(error);
    NES_ERROR("IREECompilerWrapper: Error from IREE compiler API: {}", msg)
    ireeCompilerErrorDestroy(error);
}

static void cleanupCompilerState(compilerState state)
{
    if (state.inv)
        ireeCompilerInvocationDestroy(state.inv);
    if (state.source)
        ireeCompilerSourceDestroy(state.source);
    if (state.session)
        ireeCompilerSessionDestroy(state.session);
    if (!state.hasCompiled)
        ireeCompilerGlobalShutdown();
}

iree_const_byte_span_t IREECompilerWrapper::compileModel(std::string mlirModulePath)
{
    // This line prevents "CommandLine Error: Option registered more than once!" from occurring
    llvm::cl::ResetCommandLineParser();

    ireeCompilerGlobalInitialize();

    compilerState state{};
    state.session = nullptr;
    state.source = nullptr;
    state.output = nullptr;
    state.inv = nullptr;
    state.hasCompiled = false;
    state.session = ireeCompilerSessionCreate();

    // Set flags for CPU execution
    const char* flag1 = "--iree-input-type=auto";
    const char* flag2 = "--iree-hal-target-device=llvm-cpu";
    const char* flag3 = "--iree-llvmcpu-target-cpu=host";
    const char* flag4 = "--iree-llvmcpu-debug-symbols=false"; // helps reduce binary size of the model
    const char* flag5 = "--iree-stream-partitioning-favor=min-peak-memory"; // minimize memory usage at the cost of concurrency
    ireeCompilerSessionSetFlags(state.session, 1, &flag1);
    ireeCompilerSessionSetFlags(state.session, 1, &flag2);
    ireeCompilerSessionSetFlags(state.session, 1, &flag3);
    ireeCompilerSessionSetFlags(state.session, 1, &flag4);
    ireeCompilerSessionSetFlags(state.session, 1, &flag5);

    iree_compiler_error_t *error = nullptr;
    error = ireeCompilerSourceOpenFile(state.session, mlirModulePath.c_str(), &state.source);
    if (error != nullptr)
    {
        NES_ERROR("IREECompilerWrapper: Error loading MLIR module!")
        handleCompilerError(error);
        cleanupCompilerState(state);
    }

    state.inv = ireeCompilerInvocationCreate(state.session);
    ireeCompilerInvocationEnableConsoleDiagnostics(state.inv);
    if (!ireeCompilerInvocationParseSource(state.inv, state.source))
    {
        NES_ERROR("IREECompilerWrapper: Error parsing input source into invocation!")
        cleanupCompilerState(state);
    }

    // Extract the name of the inference function to be executed
    const MlirOperation moduleOp = ireeCompilerInvocationExportStealModule(state.inv);
    const MlirRegion region = mlirOperationGetFirstRegion(moduleOp);
    const MlirOperation funcOp = mlirBlockGetFirstOperation(mlirRegionGetFirstBlock(region));
    const MlirAttribute functionNameAttr = mlirOperationGetAttributeByName(funcOp, mlirStringRefCreateFromCString("sym_name"));
    const std::string functionName = mlirStringAttrGetValue(functionNameAttr).data;
    this->functionName = "module." + functionName;

    // Extract the shape of the input tensor and calculate the size for the input buffer
    const MlirRegion nextRegion = mlirOperationGetFirstRegion(funcOp);
    const MlirBlock block = mlirRegionGetFirstBlock(nextRegion);
    const MlirValue argValue = mlirBlockGetArgument(block, 0);
    const MlirType argType = mlirValueGetType(argValue);
    const int64_t rank = torchMlirTorchValueTensorTypeGetRank(argType);
    int64_t* shape = (int64_t*) malloc(rank * sizeof(int64_t));
    torchMlirTorchValueTensorTypeGetSizes(argType, shape);
    shape[0] = 1; // fix the batch size for now

    size_t inputSize = 1;
    for (int i = 0; i < rank - 1; i++)
    {
        inputSize *= shape[i+1] * sizeof(float);
    }

    std::vector<int> inputShape;
    for (int i = 0; i < rank; i++)
    {
        inputShape.push_back(shape[i]);
    }
    free(shape);

    this->inputSize = inputSize;
    this->inputShape = inputShape;
    this->nDim = rank;

    ireeCompilerInvocationSetCompileToPhase(state.inv, "end");
    NES_INFO("Compiling the model, this might take a while")
    if (!ireeCompilerInvocationPipeline(state.inv, IREE_COMPILER_PIPELINE_STD))
    {
        NES_ERROR("IREECompilerWrapper: Error running compiler invocation!")
        cleanupCompilerState(state);
    }

    error = ireeCompilerOutputOpenMembuffer(&state.output);
    if (error != nullptr)
    {
        NES_ERROR("IREECompilerWrapper: Error opening the buffer!")
        handleCompilerError(error);
        cleanupCompilerState(state);
    }

    error = ireeCompilerInvocationOutputVMBytecode(state.inv, state.output);
    if (error != nullptr)
    {
        NES_ERROR("IREECompilerWrapper: Error outputting IREE VM bytecode!")
        handleCompilerError(error);
        cleanupCompilerState(state);
    }

    void* compiledModel = nullptr;
    uint64_t size = -1;
    error = ireeCompilerOutputMapMemory(state.output, &compiledModel, &size);
    if (error != nullptr)
    {
        NES_ERROR("IREECompilerWrapper: Error mapping IREE compiler output to memory!")
        handleCompilerError(error);
        cleanupCompilerState(state);
    }
    const iree_const_byte_span_t compiledModelConstByteSpan = iree_make_const_byte_span(compiledModel, size);
    state.hasCompiled = true;
    cleanupCompilerState(state);

    return compiledModelConstByteSpan;
}

std::string IREECompilerWrapper::getFunctionName()
{
    return this->functionName;
}

size_t IREECompilerWrapper::getInputSize()
{
    return this->inputSize;
}

std::vector<int> IREECompilerWrapper::getInputShape()
{
    return this->inputShape;
}

int IREECompilerWrapper::getNDim()
{
    return this->nDim;
}

}