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

#include <Execution/Operators/ONNX/ONNXInferenceOperatorHandler.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <absl/types/span.h>
#include <algorithm>
#include <bit>
#include <cpp-base64/base64.h>
#include <numeric>
#include <onnxruntime_cxx_api.h>

namespace NES::Runtime::Execution::Operators {

static const auto memory_info = Ort::MemoryInfo::CreateCpu(OrtDeviceAllocator, OrtMemTypeCPU);

ONNXInferenceOperatorHandlerPtr ONNXInferenceOperatorHandler::create(std::string model) {
    return std::make_shared<ONNXInferenceOperatorHandler>(std::move(model));
}

ONNXInferenceOperatorHandler::ONNXInferenceOperatorHandler(std::string model) : model(std::move(model)) {
    if (!this->model.ends_with(".onnx")) {
        throw std::runtime_error("Model file must end with .onnx");
    }

    initializeModel(this->model);
}

ONNXInferenceOperatorHandler::~ONNXInferenceOperatorHandler() { this->session->release(); }

void ONNXInferenceOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t) {}

void ONNXInferenceOperatorHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {}

void ONNXInferenceOperatorHandler::reconfigure(Runtime::ReconfigurationMessage&, Runtime::WorkerContext&) {}

void ONNXInferenceOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {}

const std::string& ONNXInferenceOperatorHandler::getModel() const { return model; }

void ONNXInferenceOperatorHandler::appendBase64EncodedData(const std::string_view& base64_encoded_data) {
    auto decoded = base64_decode(base64_encoded_data);
    std::copy(decoded.begin(), decoded.end(), std::back_inserter(input_buffer));
}

std::string ONNXInferenceOperatorHandler::getBase64EncodedData() const {
    auto data = std::string_view(ONNX_HANDLER_CAST<const char*>(output_buffer.data()), output_buffer.size());
    return base64_encode(data);
}

void ONNXInferenceOperatorHandler::infer() {
    NES_DEBUG("Buffer Size: {}  ModelInputSize: {}", input_buffer.size(), getModelInputSizeInBytes())
    NES_ASSERT(input_buffer.size() == getModelInputSizeInBytes(), "Input buffer size does not match model input size");

    inferInternal(absl::Span<int8_t>(input_buffer), absl::Span<int8_t>(output_buffer));
    input_buffer.clear();
}

void ONNXInferenceOperatorHandler::initializeModel(const std::string& pathToModel) {
    loadModel(pathToModel);
    auto input_size = getModelInputSizeInBytes();
    auto output_size = getModelOutputSizeInBytes();

    input_buffer.reserve(input_size);
    output_buffer.resize(output_size);
}

float ONNXInferenceOperatorHandler::getResultAt(size_t i) const {
    NES_ASSERT(i < output_buffer.size() / 4, "Index out of bounds");
    return *ONNX_HANDLER_CAST<const float*>(output_buffer.data() + (i * 4));
}

size_t ONNXInferenceOperatorHandler::getModelOutputSizeInBytes() {
    NES_ASSERT(this->session, "Session not initialized. Was loadModel called?");

    return std::accumulate(this->output_shape.begin(), this->output_shape.end(), 1, std::multiplies<>()) * sizeof(float);
}

size_t ONNXInferenceOperatorHandler::getModelInputSizeInBytes() {
    NES_ASSERT(this->session, "Session not initialized. Was loadModel called?");

    return std::accumulate(this->input_shape.begin(), this->input_shape.end(), 1, std::multiplies<>()) * sizeof(float);
}

void ONNXInferenceOperatorHandler::inferInternal(absl::Span<int8_t> input, absl::Span<int8_t> output) {
    NES_ASSERT(session, "Session not initialized");
    NES_ASSERT(input.size() % sizeof(float) == 0, "Input size must be a multiple of sizeof(float)");
    NES_ASSERT(output.size() % sizeof(float) == 0, "Output size must be a multiple of sizeof(float)");
    OrtAllocator* allocator;
    Ort::ThrowOnError(Ort::GetApi().GetAllocatorWithDefaultOptions(&allocator));
    static_assert(std::endian::native == std::endian::little, "TODO: Big Endian");

    auto output_data_size = output.size() / (sizeof(float) / sizeof(uint8_t));
    auto output_data = ONNX_HANDLER_CAST<float*>(output.data());
    auto input_data_size = input.size() / (sizeof(float) / sizeof(uint8_t));
    auto input_data = ONNX_HANDLER_CAST<float*>(input.data());

    auto input_tensor =
        Ort::Value::CreateTensor<float>(memory_info, input_data, input_data_size, input_shape.data(), input_shape.size());
    auto output_tensor =
        Ort::Value::CreateTensor<float>(memory_info, output_data, output_data_size, output_shape.data(), output_shape.size());

    auto input_name = session->GetInputNameAllocated(0, allocator);
    auto output_name = session->GetOutputNameAllocated(0, allocator);
    std::array<char*, 1> input_names = {input_name.get()};
    std::array<char*, 1> output_names = {output_name.get()};

    Ort::RunOptions run_options;
    session->Run(run_options, input_names.data(), &input_tensor, 1, output_names.data(), &output_tensor, 1);
}

void ONNXInferenceOperatorHandler::loadModel(const std::string& pathToModel) {
    NES_DEBUG("INITIALIZING MODEL:  {}", pathToModel);

    OrtAllocator* allocator;
    Ort::ThrowOnError(Ort::GetApi().GetAllocatorWithDefaultOptions(&allocator));
    this->session = std::make_unique<Ort::Session>(Ort::Env(), pathToModel.c_str(), Ort::SessionOptions{nullptr});

    session->GetInputCount();
    NES_DEBUG("Model: {}", pathToModel);
    NES_DEBUG("Input count: {}", session->GetInputCount());
    for (size_t i = 0; i < session->GetInputCount(); ++i) {
        auto ti = session->GetInputTypeInfo(i);
        auto shape_info = ti.GetTensorTypeAndShapeInfo();
        std::vector<int64_t> shape = shape_info.GetShape();

        if (std::any_of(shape.begin(), shape.end(), [](int64_t dim) {
                return dim == -1;
            })) {
            NES_ERROR("Model: {}, Input: {} Shape: {}", pathToModel, i, fmt::join(shape, ","));
            NES_ASSERT(false, "Dynamic shape not supported!");
        }

        NES_DEBUG("Input {} type: {}", i, ti.GetONNXType());
        NES_DEBUG("         name: {}", session->GetInputNameAllocated(i, allocator).get());
        NES_DEBUG("        shape: {}", fmt::join(shape, ","));
        NES_DEBUG(" element type: {}", shape_info.GetElementType());
        NES_DEBUG("element count: {}", shape_info.GetElementCount());
    }

    NES_DEBUG("Output count: {}", session->GetOutputCount());
    for (size_t i = 0; i < session->GetOutputCount(); ++i) {
        auto ti = session->GetOutputTypeInfo(i);
        auto shape_info = ti.GetTensorTypeAndShapeInfo();
        std::vector<int64_t> shape = shape_info.GetShape();

        if (std::any_of(shape.begin(), shape.end(), [](int64_t dim) {
                return dim == -1;
            })) {
            NES_ERROR("Model: {}, Output: {} Shape: {}", pathToModel, i, fmt::join(shape, ","));
            NES_ASSERT(false, "Dynamic shape not supported!");
        }

        NES_DEBUG("Output {} type: {}", i, ti.GetONNXType());
        NES_DEBUG("         name: {}", session->GetOutputNameAllocated(i, allocator).get());
        NES_DEBUG("         shape: {}", fmt::join(shape, ","));
        NES_DEBUG("  element type: {}", shape_info.GetElementType());
        NES_DEBUG(" element count: {}", shape_info.GetElementCount());
    }

    auto metaData = session->GetModelMetadata();
    NES_DEBUG("Producer name: {}", metaData.GetProducerNameAllocated(allocator).get());
    NES_DEBUG("Graph name: {}", metaData.GetGraphNameAllocated(allocator).get());
    NES_DEBUG("Description: {}", metaData.GetDescriptionAllocated(allocator).get());
    NES_DEBUG("Domain: {}", metaData.GetDomainAllocated(allocator).get());
    NES_DEBUG("Version: {}", metaData.GetVersion());

    NES_ASSERT(session->GetOutputCount() == 1 && session->GetInputCount() == 1, "Only single input/output models are supported");

    auto inputTypeInfo = session->GetInputTypeInfo(0);
    this->input_shape = inputTypeInfo.GetTensorTypeAndShapeInfo().GetShape();
    auto outputTypeInfo = session->GetOutputTypeInfo(0);
    this->output_shape = outputTypeInfo.GetTensorTypeAndShapeInfo().GetShape();
}
}// namespace NES::Runtime::Execution::Operators
