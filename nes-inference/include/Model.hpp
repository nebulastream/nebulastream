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

#include <cstddef>
#include <expected>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <SerializableOperator.pb.h>

namespace NES::Nebuli::Inference
{
class Model;
}

/// Forward-declare NES::Inference types and load() so Model can befriend it
namespace NES::Inference
{
struct ModelLoadError;
struct ModelOptions;
std::expected<NES::Nebuli::Inference::Model, ModelLoadError> load(const std::filesystem::path& path, const ModelOptions& options);
}

namespace NES::Nebuli::Inference
{

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

        [[nodiscard]] std::span<const std::byte> getBuffer() const { return {buffer.get(), size}; }
    };

    RefCountedByteBuffer byteCode;
    std::vector<size_t> shape;
    std::vector<size_t> outputShape;
    std::string functionName;
    size_t dims = 0;
    size_t outputDims = 0;
    size_t inputSizeInBytes = 0;
    size_t outputSizeInBytes = 0;
    std::vector<NES::DataType> inputs;
    std::vector<std::pair<std::string, NES::DataType>> outputs;

public:
    Model(std::shared_ptr<std::byte[]> modelByteCode, size_t modelSize) : byteCode(std::move(modelByteCode), modelSize) { }

    [[nodiscard]] std::span<const std::byte> getByteCode() const { return byteCode.getBuffer(); }

    [[nodiscard]] const std::vector<NES::DataType>& getInputs() const { return inputs; }

    [[nodiscard]] const std::vector<std::pair<std::string, NES::DataType>>& getOutputs() const { return outputs; }

    bool operator==(const Model&) const = default;

    [[nodiscard]] const std::vector<size_t>& getInputShape() const { return shape; }

    [[nodiscard]] const std::vector<size_t>& getOutputShape() const { return outputShape; }

    [[nodiscard]] size_t getNDim() const { return dims; }

    [[nodiscard]] size_t getOutputDims() const { return outputDims; }

    [[nodiscard]] size_t inputSize() const { return inputSizeInBytes; }

    [[nodiscard]] size_t outputSize() const { return outputSizeInBytes; }

    [[nodiscard]] const std::string& getFunctionName() const { return functionName; }

    friend std::expected<Model, NES::Inference::ModelLoadError>
    NES::Inference::load(const std::filesystem::path& path, const NES::Inference::ModelOptions& options);
    friend Model deserializeModel(const SerializableOperator_Model& grpcModel);
    friend void serializeModel(const Model& model, SerializableOperator_Model& target);
};

Model deserializeModel(const SerializableOperator_Model& grpcModel);
void serializeModel(const Model& model, SerializableOperator_Model& target);
}
