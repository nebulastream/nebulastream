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
#include <SerializableOperator.pb.h>
#include <Common/DataTypes/DataType.hpp>

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
        std::span<const std::byte> getBuffer() const { return {buffer.get(), size}; }
    };

    RefCountedByteBuffer byteCode;
    std::vector<int> shape;
    std::string functionName = "module.main_graph";
    size_t dims = 0;
    size_t inputSizeInBytes = 0;
    std::vector<std::shared_ptr<NES::DataType>> inputs;
    std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>> outputs;

public:
    Model(std::shared_ptr<std::byte[]> modelByteCode, size_t modelSize) : byteCode(std::move(modelByteCode), modelSize) { }

    std::span<const std::byte> getByteCode() const { return byteCode.getBuffer(); }
    const std::vector<std::shared_ptr<NES::DataType>>& getInputs() const { return inputs; }
    const std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>>& getOutputs() const { return outputs; }

    bool operator==(const Model&) const = default;

    const std::vector<int>& getInputShape() { return shape; }
    size_t getNDim() { return dims; }
    size_t inputSize() { return inputSizeInBytes; }
    const std::string& getFunctionName() { return functionName; }

    friend class ModelCatalog;
    friend Model deserializeModel(const SerializableOperator_Model& grpcModel);
    friend void serializeModel(const Model& model, SerializableOperator_Model& target);
};

Model deserializeModel(const SerializableOperator_Model& grpcModel);
void serializeModel(const Model& model, SerializableOperator_Model& target);
}