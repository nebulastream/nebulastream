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

#ifndef NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWINFERENCEOPERATOR_HPP_
#define NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWINFERENCEOPERATOR_HPP_

#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief this is the nautilus implementation of infer model operator. This operator allows for inferring (currently only tensorflow) machine learning model over incoming data stream.
 */
class TensorflowInferenceOperator : public ExecutableOperator {

  public:
    /**
     * @brief Creates a infer model operator.
     * @param inferModelHandlerIndex index of infer model handler.
     * @param inputFieldNames names of input fields for the model inference
     * @param outputFieldNames names of output fields from the model inference
     */
    TensorflowInferenceOperator(const uint32_t inferModelHandlerIndex,
                                const std::vector<std::string>& inputFieldNames,
                                const std::vector<std::string>& outputFieldNames)
        : inferModelHandlerIndex(inferModelHandlerIndex), inputFieldNames(inputFieldNames), outputFieldNames(outputFieldNames){};

    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const uint32_t inferModelHandlerIndex;
    const std::vector<std::string> inputFieldNames;
    const std::vector<std::string> outputFieldNames;
};

}// namespace NES::Runtime::Execution::Operators
#endif // NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWINFERENCEOPERATOR_HPP_
