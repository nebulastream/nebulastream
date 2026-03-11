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

#pragma once

#include <memory>
#include <vector>
#include <string>
#include <span>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>

#include <DataTypes/DataType.hpp>

namespace NES::Inference
{
struct ModelLoadError;
struct ModelOptions;

class Model
{
    struct RefCountedByteBuffer
    {
        std::shared_ptr<std::byte[]> buffer;
        size_t size = 0;
        friend bool operator==(const RefCountedByteBuffer& lhs, const RefCountedByteBuffer& rhs)
        {
            return std::ranges::equal(std::span{lhs.buffer.get(), lhs.size}, std::span{rhs.buffer.get(), rhs.size});
        }
        friend bool operator!=(const RefCountedByteBuffer& lhs, const RefCountedByteBuffer& rhs) { return !(lhs == rhs); }
        std::span<const std::byte> getBuffer() const { return {buffer.get(), size}; }
    };

    RefCountedByteBuffer byteCode;
    std::vector<size_t> shape;
    std::vector<size_t> outputShape;
    std::string functionName;
    size_t dims = 0;
    size_t outputDims = 0;
    size_t inputSizeInBytes = 0;
    size_t outputSizeInBytes = 0;
    std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>> inputs;
    std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>> outputs;

public:
    Model() = default;
    Model(std::shared_ptr<std::byte[]> modelByteCode, size_t modelSize) : byteCode(std::move(modelByteCode), modelSize) { }

    std::span<const std::byte> getByteCode() const { return byteCode.getBuffer(); }
    const std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>>& getInputs() const { return inputs; }
    const std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>>& getOutputs() const { return outputs; }

    bool operator==(const Model& rhs) const;

    const std::vector<size_t>& getInputShape() const { return shape; }
    const std::vector<size_t>& getOutputShape() const { return outputShape; }

    size_t getNDim() const { return dims; }
    size_t getOutputDims() const { return outputDims; }

    size_t inputSize() const { return inputSizeInBytes; }
    size_t outputSize() const { return outputSizeInBytes; }

    const std::string& getFunctionName() const { return functionName; }

    void setInputs(std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>> newInputs) { inputs = std::move(newInputs); }
    void setOutputs(std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>> newOutputs) { outputs = std::move(newOutputs); }
    void setFunctionName(std::string name) { functionName = std::move(name); }
    void setInputShape(std::vector<size_t> s) { shape = std::move(s); dims = shape.size(); }
    void setOutputShape(std::vector<size_t> s) { outputShape = std::move(s); outputDims = outputShape.size(); }
    void setInputSizeInBytes(size_t s) { inputSizeInBytes = s; }
    void setOutputSizeInBytes(size_t s) { outputSizeInBytes = s; }

    friend class ModelCatalog;
    friend std::expected<Model, ModelLoadError> load(const std::filesystem::path& path, const ModelOptions& options);
};

}
