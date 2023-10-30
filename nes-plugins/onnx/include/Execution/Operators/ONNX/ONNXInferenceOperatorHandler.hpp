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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_INFERMODEL_INFERMODELHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_INFERMODEL_INFERMODELHANDLER_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <absl/types/span.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

//TODO: Change to BitCast once available
#define ONNX_HANDLER_CAST reinterpret_cast

namespace Ort {
class Session;
}

namespace NES::Runtime::Execution::Operators {
class ONNXInferenceOperatorHandler;
using ONNXInferenceOperatorHandlerPtr = std::shared_ptr<ONNXInferenceOperatorHandler>;

/**
 * @brief OperatorHandler for the ModelInferenceOperator using the ONNX Runtime
 * @details This class is responsible for loading and managing the model (which includes the ONNX Runtime Session)
 *          and loading the input and output tensors into the model
 */
class ONNXInferenceOperatorHandler : public OperatorHandler {
  public:
    static ONNXInferenceOperatorHandlerPtr create(std::string model);

    explicit ONNXInferenceOperatorHandler(std::string model);

    ~ONNXInferenceOperatorHandler() override;

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override;

    /**
     * @brief returns the model name
     * @return reference to the path of the Model as string
     */
    const std::string& getModel() const;

    /**
     * @brief Initialize the Model
     * @param pathToModel file containing the serialized model
     */
    void initializeModel(const std::string& pathToModel);

    /**
     * @brief Runs inference on the Model for a single input tuple
     */
    void infer();

    /**
     * @brief accesses the ith field of the output
     * @param i index of the output value
     * @return float value
     */
    [[nodiscard]] float getResultAt(size_t i) const;

    /**
     * @brief loads base64 encoded data into the input tensor
     * @param string_view into the base64 encoded data. Data is copied!
     */
    void appendBase64EncodedData(const std::string_view& base64_encoded_data);

    /**
     * @brief copies the output of the inference model into a base64 encoded string
     * @return base64 encoded output of the model
     */
    [[nodiscard]] std::string getBase64EncodedData() const;

    /**
     * Copies a single objects of type T into the input tensor
     * @tparam T (float, uint*_t, int*_t)
     * @param data pointer to object
     */
    template<typename T>
    void appendToByteArray(const T& data);

  private:
    void inferInternal(absl::Span<int8_t> input, absl::Span<int8_t> output);

    void loadModel(const std::string& pathToModel);

    size_t getModelOutputSizeInBytes();

    size_t getModelInputSizeInBytes();

    std::string model;
    std::vector<int8_t> output_buffer;
    std::vector<int8_t> input_buffer;
    std::unique_ptr<Ort::Session> session;
    std::vector<int64_t> input_shape;
    std::vector<int64_t> output_shape;
};

template<typename T>
void ONNXInferenceOperatorHandler::appendToByteArray(const T& data) {
    auto* ptr = ONNX_HANDLER_CAST<const int8_t*>(&data);
    input_buffer.insert(input_buffer.end(), ptr, ptr + sizeof(T));
}
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_INFERMODEL_INFERMODELHANDLER_HPP_
