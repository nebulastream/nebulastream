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

#include <Execution/Operators/Experimental/Vectorization/Kernel.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Backends/Experimental/Vectorization/KernelExecutable.hpp>
#include <Nautilus/Backends/Experimental/Vectorization/CUDA/CUDAKernelCompiler.hpp>

namespace NES::Runtime::Execution::Operators {

void invokeKernel(void* kernelExecutable, void* recordBuffer) {
    auto executable = static_cast<Backends::KernelExecutable*>(kernelExecutable);
    auto kernelWrapper = executable->getKernelWrapperFunction();
    kernelWrapper(recordBuffer);
}

Kernel::Kernel(Descriptor descriptor)
    : descriptor(descriptor)
    , kernelExecutable(nullptr)
{

}

void Kernel::setup(ExecutionContext& ctx) const {
    auto desc = Nautilus::Backends::CUDA::CUDAKernelCompiler::Descriptor {
        .kernelFunctionName = "cudaKernel",
        .wrapperFunctionName = "cudaKernelWrapper",
        .inputSchemaSize = descriptor.inputSchemaSize,
        .threadsPerBlock = descriptor.threadsPerBlock,
    };
    auto cudaKernelCompiler = Nautilus::Backends::CUDA::CUDAKernelCompiler(desc);
    auto compileOptions = descriptor.compileOptions;
    auto executable = cudaKernelCompiler.compile(descriptor.pipeline, compileOptions);
    // TODO(#4148) Assign kernel executable

    Operator::setup(ctx);
    auto pipeline = descriptor.pipeline;
    pipeline->setup(ctx);
}

void Kernel::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    Operator::open(ctx, recordBuffer);
    auto pipeline = descriptor.pipeline;
    pipeline->open(ctx, recordBuffer);
}

void Kernel::execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    auto kernelExecutableRef = Value<MemRef>((int8_t*) kernelExecutable.get());
    auto recordBufferRef = recordBuffer.getReference();
    Nautilus::FunctionCall("invokeKernel", invokeKernel, kernelExecutableRef, recordBufferRef);
    auto vectorizedChild = std::dynamic_pointer_cast<const VectorizableOperator>(child);
    vectorizedChild->execute(ctx, recordBuffer);
}

} // namespace NES::Runtime::Execution::Operators