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

#ifndef NES_PLUGINS_ONNX_INCLUDE_EXECUTION_OPERATORS_ONNX_ONNXINFERENCEOPERATOR_HPP_
#define NES_PLUGINS_ONNX_INCLUDE_EXECUTION_OPERATORS_ONNX_ONNXINFERENCEOPERATOR_HPP_

#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief this is the nautilus implementation of infer model operator. This operator allows for inferring  machine learning model
 * over incoming data stream, using the ONNX Runtime.
 * @details Creation of the Operator is handled by the ONNXOperatorLoweringPlugin, which when given a PhysicalModelInferenceOperator
 * with a ONNX model (determined by the .onnx suffix) will create a Nautilus ONNXInferenceOperator
 */
class ONNXInferenceOperator : public ExecutableOperator {

  public:
    /**
     * Nautilus Operator for Model Inference using the ONNX Runtime.
     * @param inferModelHandlerIndex Index of the ONNXInferenceOperatorHandler
     * @param inputFieldNames vector of strings for the input field names
     * @param outputFieldNames  vector of string for the output field names
     */
    ONNXInferenceOperator(const uint32_t inferModelHandlerIndex,
                          const std::vector<std::string>& inputFieldNames,
                          const std::vector<std::string>& outputFieldNames)
        : inferModelHandlerIndex(inferModelHandlerIndex), inputFieldNames(inputFieldNames), outputFieldNames(outputFieldNames){};

    /**
     * @brief Copies tuples into the input tensor, runs the model, copies data out of the output tensor
     * @details The operator behaves differently based on the output records schema:
     *  - IFF the output schema contains a single TEXT field, named "data", the entire output tensor will be encoded as base64
     *    and written to the output records "data" field.
     *  - Otherwise data is read value by value from the output tensor and written to the output records, which might drop some
     *    trailing values.
     * @param ctx Nautilus Execution Context
     * @param record Record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const uint32_t inferModelHandlerIndex;
    const std::vector<std::string> inputFieldNames;
    const std::vector<std::string> outputFieldNames;
};

}// namespace NES::Runtime::Execution::Operators
#endif // NES_PLUGINS_ONNX_INCLUDE_EXECUTION_OPERATORS_ONNX_ONNXINFERENCEOPERATOR_HPP_
