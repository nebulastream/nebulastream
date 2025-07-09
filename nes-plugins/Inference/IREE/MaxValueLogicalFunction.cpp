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
#include <Util/Common.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

class MaxValueLogicalFunction : public LogicalFunctionConcept
{
    LogicalFunction child;
    DataType type = DataTypeProvider::provideDataType(DataType::Type::FLOAT32);

public:
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override
    {
        return fmt::format("MAX_VALUE({})", child.explain(verbosity));
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

        if (!copy.child.getDataType().isType(DataType::Type::VARSIZED))
        {
            throw TypeInferenceException("Function Expects 1 Argument with Varsized input");
        }

        copy.type = DataTypeProvider::provideDataType(DataType::Type::FLOAT32);
        return copy;
    }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override { return {child}; }
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override
    {
        auto copy = *this;
        copy.child = children.at(0);
        return copy;
    }
    [[nodiscard]] std::string_view getType() const override { return "MaxValue"; }
    [[nodiscard]] SerializableFunction serialize() const override
    {
        SerializableFunction serializedFunction;
        serializedFunction.set_function_type(getType());
        *serializedFunction.add_children() = child.serialize();
        return serializedFunction;
    }

    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override
    {
        if (const auto* casted = dynamic_cast<const MaxValueLogicalFunction*>(&rhs))
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
LogicalFunctionRegistryReturnType RegisterMaxValueLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw TypeInferenceException("Function Expects 1 Argument");
    }

    return LogicalFunction(MaxValueLogicalFunction{}.withChildren(std::move(arguments.children)));
}
}
