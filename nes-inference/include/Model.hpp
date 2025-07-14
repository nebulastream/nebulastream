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
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>

#include <SerializableVariantDescriptor.pb.h>
#include <DataTypes/DataType.hpp>

namespace NES::Nebuli::Inference
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
    std::vector<DataType> inputs;
    std::vector<std::pair<std::string, DataType>> outputs;

public:
    Model(std::shared_ptr<std::byte[]> modelByteCode, size_t modelSize) : byteCode(std::move(modelByteCode), modelSize) { }

    std::span<const std::byte> getByteCode() const { return byteCode.getBuffer(); }
    const std::vector<DataType>& getInputs() const { return inputs; }
    const std::vector<std::pair<std::string, DataType>>& getOutputs() const { return outputs; }

    bool operator==(const Model&) const = default;

    const std::vector<size_t>& getInputShape() { return shape; }
    const std::vector<size_t>& getOutputShape() { return outputShape; }

    size_t getNDim() { return dims; }
    size_t getOutputDims() { return outputDims; }

    size_t inputSize() const { return inputSizeInBytes; }
    size_t outputSize() const { return outputSizeInBytes; }

    const std::string& getFunctionName() { return functionName; }

    friend class ModelCatalog;
    friend std::expected<Model, ModelLoadError> load(const std::filesystem::path& path, const ModelOptions& options);
    friend Model deserializeModel(const SerializableModel& grpcModel);
    friend void serializeModel(const Model& model, SerializableModel& target);
};

Model deserializeModel(const SerializableModel& grpcModel);
void serializeModel(const Model& model, SerializableModel& target);
}
