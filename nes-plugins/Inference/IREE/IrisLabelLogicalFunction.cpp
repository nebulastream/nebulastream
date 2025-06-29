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

#include <memory>

#include <LogicalFunctionRegistry.hpp>
#include <Functions/LogicalFunction.hpp>
#include "DataTypes/DataTypeProvider.hpp"
#include "Functions/LogicalFunction.hpp"
#include "Serialization/DataTypeSerializationUtil.hpp"

namespace NES
{

class IrisLabelLogicalFunction : public LogicalFunctionConcept
{
    LogicalFunction setosa;
    LogicalFunction versicolor;
    LogicalFunction virginica;

public:
    IrisLabelLogicalFunction(LogicalFunction setosa, LogicalFunction versicolor, LogicalFunction virginica)
        : setosa(std::move(setosa)), versicolor(std::move(versicolor)), virginica(std::move(virginica))
    {
    }
    [[nodiscard]] std::string explain(ExplainVerbosity) const override { return "IRIS_LABEL"; }
    [[nodiscard]] DataType getDataType() const override { return DataTypeProvider::provideDataType(DataType::Type::VARSIZED); }
    [[nodiscard]] LogicalFunction withDataType(const DataType&) const override { return *this; }
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override
    {
        auto copy = IrisLabelLogicalFunction(
            setosa.withInferredDataType(schema), versicolor.withInferredDataType(schema), virginica.withInferredDataType(schema));

        if (copy.setosa.getDataType().type != DataType::Type::FLOAT32 || copy.versicolor.getDataType().type != DataType::Type::FLOAT32
            || copy.virginica.getDataType().type != DataType::Type::FLOAT32)
        {
            throw TypeInferenceException("IrisLabel expects Float32 as input");
        }

        return copy;
    }
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override { return {setosa, versicolor, virginica}; }
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override
    {
        PRECONDITION(children.size() == 3, "IrisLabel expects 3 children");
        return IrisLabelLogicalFunction(children.at(0), children.at(1), children.at(2));
    }

    [[nodiscard]] std::string_view getType() const override { return "iris_label"; }
    [[nodiscard]] SerializableFunction serialize() const override
    {
        SerializableFunction serializedFunction;
        serializedFunction.set_function_type("iris_label");
        serializedFunction.add_children()->CopyFrom(setosa.serialize());
        serializedFunction.add_children()->CopyFrom(versicolor.serialize());
        serializedFunction.add_children()->CopyFrom(virginica.serialize());
        DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
        return serializedFunction;
    }

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override
    {
        if (auto castedRhs = dynamic_cast<const IrisLabelLogicalFunction*>(&rhs))
        {
            return castedRhs->setosa == this->setosa && castedRhs->versicolor == this->versicolor
                && castedRhs->virginica == this->virginica;
        }
        return false;
    }
};

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::Registeriris_labelLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.children.size() == 3, "IrisLabelLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return IrisLabelLogicalFunction(arguments.children[0], arguments.children[1], arguments.children[2]);
}

}
