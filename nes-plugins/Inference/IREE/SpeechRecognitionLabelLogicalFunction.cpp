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

#include <DataTypes/DataTypeProvider.hpp>

#include <LogicalFunctionRegistry.hpp>

namespace NES
{

class SpeechRecognitionLabelLogicalFunction final : public LogicalFunctionConcept
{
    LogicalFunction child;
    DataType type = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);

public:
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override
    {
        return fmt::format("SPEECH_RECOGNITION_LABEL({})", child.explain(verbosity));
    }

    [[nodiscard]] DataType getDataType() const override { return type; }

    [[nodiscard]] LogicalFunction withDataType(const DataType& dataType) const override
    {
        auto copy = *this;
        copy.type = dataType;
        return copy;
    }

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override
    {
        auto copy = *this;
        copy.child = child.withInferredDataType(schema);

        if (!copy.child.getDataType().isInteger())
        {
            throw TypeInferenceException("Function Expects 1 Argument with Integer input");
        }
        copy.type = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
        return copy;
    }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override { return {child}; }
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override
    {
        auto copy = *this;
        copy.child = children.at(0);
        return copy;
    }
    [[nodiscard]] std::string_view getType() const override { return "SpeechRecognitionLabel"; }
    [[nodiscard]] SerializableFunction serialize() const override
    {
        SerializableFunction serializedFunction;
        serializedFunction.set_function_type(getType());
        *serializedFunction.add_children() = child.serialize();
        return serializedFunction;
    }

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override
    {
        if (const auto* casted = dynamic_cast<const SpeechRecognitionLabelLogicalFunction*>(&rhs))
        {
            return child == casted->child;
        }
        return false;
    }
};

}

namespace NES::LogicalFunctionGeneratedRegistrar
{
/// declaration of register functions for 'LogicalFunctions'
LogicalFunctionRegistryReturnType RegisterSpeechRecognitionLabelLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw TypeInferenceException("Function Expects 1 Argument");
    }

    return LogicalFunction(SpeechRecognitionLabelLogicalFunction{}.withChildren(std::move(arguments.children)));
}
}
