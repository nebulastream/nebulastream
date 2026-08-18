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

#include <OpenVinoRuntimeBackend.hpp>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Model.hpp>
#include <RuntimeBackend.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ios>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
#include <openvino/core/shape.hpp>
#include <openvino/core/type/element_type.hpp>
#include <openvino/core/type/float16.hpp>
#include <openvino/runtime/core.hpp>
#include <openvino/runtime/properties.hpp>
#include <openvino/runtime/tensor.hpp>

namespace NES
{

namespace
{
/// Process-wide OpenVINO runtime: one `ov::Core` and the models compiled from it, behind one
/// lock. The core, its cache, and the mutex share a single lifetime and are torn down together at process exit.
///
/// Every worker thread running the same operator calls `setup` with the same model, and
/// `read_model`/`compile_model` takes seconds for a large one, so a compiled model is cached and reused across threads.
class OpenVinoRuntime
{
public:
    static OpenVinoRuntime& instance()
    {
        static OpenVinoRuntime runtime;
        return runtime;
    }

    ov::CompiledModel getOrCompile(const CompiledModel& model)
    {
        const std::scoped_lock lock(mutex);

        /// The payload buffer is ref-counted and shared by every copy of a model, so its address identifies the model.
        /// An entry whose `weak_ptr` locks to the same buffer is a hit, and one whose `weak_ptr` has expired
        /// belongs to a model that has been destroyed.
        const auto* payloadOwner = model.getBackendModel().modelGraph.buffer.get();
        for (auto entry = cache.begin(); entry != cache.end();)
        {
            const auto pinned = entry->payload.lock();
            if (!pinned)
            {
                entry = cache.erase(entry);
                continue;
            }
            if (pinned.get() == payloadOwner)
            {
                return entry->compiled;
            }
            ++entry;
        }

        const auto& backendModel = model.getBackendModel();
        const auto modelGraphBytes = backendModel.modelGraphView();
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) byte-to-text for OpenVINO XML payload
        const std::string modelXml(reinterpret_cast<const char*>(modelGraphBytes.data()), modelGraphBytes.size());
        const auto modelWeightsBytes = backendModel.modelWeightsView();
        std::vector<std::uint8_t> modelBin(modelWeightsBytes.size());
        std::ranges::transform(modelWeightsBytes, modelBin.begin(), [](std::byte value) { return static_cast<std::uint8_t>(value); });

        const ov::Shape modelInputShape(model.getInputShape().begin(), model.getInputShape().end());
        ov::Tensor weights(ov::element::u8, {modelBin.size()});
        if (!modelBin.empty())
        {
            std::memcpy(weights.data<std::uint8_t>(), modelBin.data(), modelBin.size());
        }

        auto openVinoModel = core.read_model(modelXml, weights);
        openVinoModel->reshape(modelInputShape);

        auto compiledModel = core.compile_model(
            openVinoModel,
            "CPU",
            ov::hint::execution_mode(ov::hint::ExecutionMode::ACCURACY),
            ov::hint::performance_mode(ov::hint::PerformanceMode::LATENCY));

        cache.push_back(CacheEntry{.payload = backendModel.modelGraph.buffer, .compiled = compiledModel});
        return compiledModel;
    }

private:
    OpenVinoRuntime() = default;

    struct CacheEntry
    {
        /// NOLINTNEXTLINE(modernize-avoid-c-arrays) mirrors the model payload's array buffer
        std::weak_ptr<const std::byte[]> payload;
        ov::CompiledModel compiled;
    };

    ov::Core core;
    std::mutex mutex;
    std::vector<CacheEntry> cache;
};
}

RuntimeMetadata OpenVinoRuntimeBackend::setup(const CompiledModel& model)
{
    auto compiledModel = OpenVinoRuntime::instance().getOrCompile(model);

    inferRequest = compiledModel.create_infer_request();
    inputElementType = compiledModel.input(0).get_element_type();
    inputShape = compiledModel.input(0).get_shape();
    outputElementType = compiledModel.output(0).get_element_type();
    outputShape = compiledModel.output(0).get_shape();
    /// When other element types are supported, use inputElementType.size()/outputElementType.size() instead of sizeof(float).
    requiredInputSize = sizeof(float) * ov::shape_size(inputShape);
    requiredOutputSize = sizeof(float) * ov::shape_size(outputShape);

    return RuntimeMetadata{
        .inputShape = model.getInputShape(),
        .nDim = model.getNDim(),
        .functionName = model.getFunctionName(),
        .inputSize = model.inputSize(),
        .outputSize = model.outputSize()};
}

void OpenVinoRuntimeBackend::infer(std::byte* inputBuffer, size_t inputBufferSize, std::byte* outputBuffer, size_t outputBufferSize)
{
    if (inputBufferSize < requiredInputSize)
    {
        throw NES::InferenceRuntimeFailure(
            "Model Execution failed. Buffer capacity {} B is insufficient for model input size {} B", inputBufferSize, requiredInputSize);
    }

    if (outputBufferSize < requiredOutputSize)
    {
        throw NES::InferenceRuntimeFailure(
            "Model Execution failed. Buffer capacity {} B is insufficient for model output size {} B",
            outputBufferSize,
            requiredOutputSize);
    }

    const ov::Tensor inputTensor(inputElementType, inputShape, inputBuffer);
    inferRequest.set_input_tensor(inputTensor);

    const ov::Tensor outputTensor(outputElementType, outputShape, outputBuffer);
    inferRequest.set_output_tensor(0, outputTensor);

    inferRequest.infer();
}
}
