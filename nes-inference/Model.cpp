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

#include <Model.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <SerializableOperator.pb.h>

#include <cstddef>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

NES::Nebuli::Inference::Model NES::Nebuli::Inference::deserializeModel(const SerializableOperator_Model& grpcModel)
{
    auto modelByteCodeSize = grpcModel.bytecode().size();
    auto modelByteCodeBuffer = std::make_shared<std::byte[]>(modelByteCodeSize);
    std::ranges::copy(
        grpcModel.bytecode() | std::views::transform([](const auto& character) { return static_cast<std::byte>(character); }),
        modelByteCodeBuffer.get());

    Model model{modelByteCodeBuffer, modelByteCodeSize};

    model.functionName = grpcModel.functionname();
    model.dims = static_cast<size_t>(grpcModel.dims());
    model.shape.assign(grpcModel.shape().begin(), grpcModel.shape().end());

    model.outputDims = static_cast<size_t>(grpcModel.outputdims());
    model.outputShape.assign(grpcModel.outputshape().begin(), grpcModel.outputshape().end());

    model.inputSizeInBytes = static_cast<size_t>(grpcModel.inputsizeinbytes());
    model.outputSizeInBytes = static_cast<size_t>(grpcModel.outputsizeinbytes());

    model.inputs = grpcModel.inputs()
        | std::views::transform([](const auto& serializedDataType)
                                { return DataTypeSerializationUtil::deserializeDataType(serializedDataType); })
        | std::ranges::to<std::vector>();

    model.outputs = grpcModel.outputs()
        | std::views::transform(
                        [](const auto& typeWithName) -> std::pair<std::string, NES::DataType>
                        {
                            return {typeWithName.name(), DataTypeSerializationUtil::deserializeDataType(typeWithName.type())};
                        })
        | std::ranges::to<std::vector>();

    return model;
}

void NES::Nebuli::Inference::serializeModel(const NES::Nebuli::Inference::Model& model, SerializableOperator_Model& target)
{
    auto modelBytes = model.getByteCode() | std::views::transform([](const std::byte& byte) { return static_cast<char>(byte); });
    target.mutable_bytecode()->assign(modelBytes.begin(), modelBytes.end());
    target.set_dims(static_cast<int32_t>(model.dims));
    for (auto sh : model.shape)
    {
        target.add_shape(static_cast<int32_t>(sh));
    }

    target.set_outputdims(static_cast<int32_t>(model.outputDims));
    for (auto sh : model.outputShape)
    {
        target.add_outputshape(static_cast<int32_t>(sh));
    }

    target.set_functionname(model.functionName);
    target.set_inputsizeinbytes(static_cast<int32_t>(model.inputSizeInBytes));
    target.set_outputsizeinbytes(static_cast<int32_t>(model.outputSizeInBytes));

    for (const auto& input : model.inputs)
    {
        DataTypeSerializationUtil::serializeDataType(input, target.add_inputs());
    }

    for (const auto& [name, type] : model.outputs)
    {
        auto* output = target.add_outputs();
        output->set_name(name);
        DataTypeSerializationUtil::serializeDataType(type, output->mutable_type());
    }
}
