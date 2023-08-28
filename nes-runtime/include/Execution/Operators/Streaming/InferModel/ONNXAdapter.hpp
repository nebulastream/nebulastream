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

#include "InferenceAdapter.hpp"

#ifdef ONNXDEF
#include <string>
#include <onnxruntime/core/session/onnxruntime_cxx_api.h>
#include <optional>
#endif// ONNXDEF

namespace NES::Runtime::Execution::Operators {
class ONNXAdapter : public InferenceAdapter {
#ifdef ONNXDEF
  public:
    static InferenceAdapterPtr create();
    ONNXAdapter() = default;
    ~ONNXAdapter() override;

  protected:
    void inferInternal(std::span<int8_t>&& input, std::span<int8_t>&& output) override;
    void loadModel(const std::string& pathToModel) override;
    size_t getModelOutputSizeInBytes() override;
    size_t getModelInputSizeInBytes() override;

  private:
    std::optional<Ort::Env> env;
    std::optional<Ort::Session> session;
    std::vector<int64_t> input_shape;
    std::vector<int64_t> output_shape;
    OrtAllocator* allocator = nullptr;
#endif// ONNXDEF
};
}// namespace NES::Runtime::Execution::Operators