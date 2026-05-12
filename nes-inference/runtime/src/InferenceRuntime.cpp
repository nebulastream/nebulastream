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

#include <InferenceRuntime.hpp>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <ErrorHandling.hpp>
#ifdef NES_ENABLE_INFERENCE_BACKEND_IREE
    #include <IreeRuntimeBackend.hpp>
#endif
#include <Model.hpp>
#ifdef NES_ENABLE_INFERENCE_BACKEND_OPENVINO
    #include <OpenVinoRuntimeBackend.hpp>
#endif
#include <RuntimeBackend.hpp>

namespace NES
{

namespace
{

std::unique_ptr<RuntimeBackend> createRuntimeBackend(ModelBackend backend)
{
    switch (backend)
    {
        case ModelBackend::IREE:
#ifdef NES_ENABLE_INFERENCE_BACKEND_IREE
            return std::make_unique<IreeRuntimeBackend>();
#else
            throw NES::InferenceRuntimeFailure("IREE inference backend was not built");
#endif
        case ModelBackend::OPENVINO:
#ifdef NES_ENABLE_INFERENCE_BACKEND_OPENVINO
            return std::make_unique<OpenVinoRuntimeBackend>();
#else
            throw NES::InferenceRuntimeFailure("OpenVINO inference backend was not built");
#endif
    }
    std::unreachable();
}

}

struct InferenceRuntime::Impl
{
    std::unique_ptr<RuntimeBackend> backend;
};

InferenceRuntime::InferenceRuntime() : impl(std::make_unique<Impl>())
{
}

InferenceRuntime::~InferenceRuntime() = default;
InferenceRuntime::InferenceRuntime(InferenceRuntime&&) noexcept = default;
InferenceRuntime& InferenceRuntime::operator=(InferenceRuntime&&) noexcept = default;

void InferenceRuntime::setup(const CompiledModel& model)
{
    impl->backend = createRuntimeBackend(model.getBackend());
    auto metadata = impl->backend->setup(model);

    this->inputShape = std::move(metadata.inputShape);
    this->nDim = metadata.nDim;
    this->functionName = std::move(metadata.functionName);

    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    this->inputData = std::make_unique<std::byte[]>(metadata.inputSize);
    this->inputSize = metadata.inputSize;
    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    this->outputData = std::make_unique<std::byte[]>(metadata.outputSize);
    this->outputSize = metadata.outputSize;
}

void InferenceRuntime::infer()
{
    if (impl->backend == nullptr)
    {
        throw NES::InferenceRuntimeFailure("Model Execution failed. Runtime backend was not set up");
    }

    impl->backend->infer(inputData.get(), inputSize, outputData.get(), outputSize);
}

}
