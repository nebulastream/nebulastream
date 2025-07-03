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

#include <boost/fusion/sequence/io/out.hpp>


#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Model.hpp>
NES::Nebuli::Inference::Model NES::Nebuli::Inference::deserializeModel(const SerializableModel& grpcModel)
{
    auto modelByteCodeSize = grpcModel.bytecode().size();
    auto modelByteCodeBuffer = std::make_shared<std::byte[]>(modelByteCodeSize);
    std::ranges::copy(
        grpcModel.bytecode() | std::views::transform([](const auto& character) { return static_cast<std::byte>(character); }),
        modelByteCodeBuffer.get());

    Model model{modelByteCodeBuffer, modelByteCodeSize};

    model.functionName = grpcModel.functionname();
    model.dims = grpcModel.dims();
    model.shape.assign(grpcModel.shape().begin(), grpcModel.shape().end());

    model.outputDims = grpcModel.outputdims();
    model.outputShape.assign(grpcModel.outputshape().begin(), grpcModel.outputshape().end());

    model.inputSizeInBytes = grpcModel.inputsizeinbytes();
    model.outputSizeInBytes = grpcModel.outputsizeinbytes();
    model.inputs = grpcModel.inputs()
        | std::views::transform([](const auto& serializedDataType)
                                { return DataTypeSerializationUtil::deserializeDataType(serializedDataType); })
        | std::ranges::to<std::vector>();

    model.outputs = grpcModel.outputs()
        | std::views::transform(
                        [](const auto& typeWithName)
                        {
                            return std::pair<std::string, DataType>{
                                typeWithName.name(), DataTypeSerializationUtil::deserializeDataType(typeWithName.type())};
                        })
        | std::ranges::to<std::vector>();

    return model;
}

void NES::Nebuli::Inference::serializeModel(const Model& model, SerializableModel& target)
{
    auto modelBytes = model.getByteCode() | std::views::transform([](const std::byte& byte) { return static_cast<const char>(byte); });
    target.mutable_bytecode()->assign(modelBytes.begin(), modelBytes.end());
    target.set_dims(model.dims);
    for (int shape : model.shape)
    {
        target.add_shape(shape);
    }

    target.set_outputdims(model.outputDims);
    for (int shape : model.outputShape)
    {
        target.add_outputshape(shape);
    }

    target.set_functionname(model.functionName);
    target.set_inputsizeinbytes(model.inputSizeInBytes);
    target.set_outputsizeinbytes(model.outputSizeInBytes);
    for (auto& input : model.inputs)
    {
        DataTypeSerializationUtil::serializeDataType(input, target.add_inputs());
    }

    for (auto& [name, type] : model.outputs)
    {
        auto* output = target.add_outputs();
        output->set_name(name);
        DataTypeSerializationUtil::serializeDataType(type, output->mutable_type());
    }
}
