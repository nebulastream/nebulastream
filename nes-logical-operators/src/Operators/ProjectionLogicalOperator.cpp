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

#include <Operators/ProjectionLogicalOperator.hpp>

#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

ProjectionLogicalOperator::ProjectionLogicalOperator(std::vector<LogicalFunction> functions) : functions(std::move(std::move(functions)))
{
    const auto functionTypeNotSupported = [](const LogicalFunction& function) -> bool
    { return not(function.tryGet<FieldAccessLogicalFunction>() or function.tryGet<FieldAssignmentLogicalFunction>()); };
    bool allFunctionsAreSupported = true;
    for (const auto& function : std::views::all(this->functions) | std::views::filter(functionTypeNotSupported))
    {
        NES_ERROR("The projection operator does not support the function: {}", function);
        allFunctionsAreSupported = false;
    }
    INVARIANT(
        allFunctionsAreSupported,
        "The projection operator only supports FieldAccessLogicalFunction and FieldAssignmentLogicalFunction functions.");
}

std::string_view ProjectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

const std::vector<LogicalFunction>& ProjectionLogicalOperator::getFunctions() const
{
    return functions;
}

bool ProjectionLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const ProjectionLogicalOperator*>(&rhs))
    {
        if (functions.size() != rhsOperator->functions.size())
        {
            return false;
        }

        for (size_t i = 0; i < functions.size(); i++)
        {
            if (functions[i] != rhsOperator->functions[i])
            {
                return false;
            }
        }

        return getOutputSchema() == rhsOperator->getOutputSchema() && getInputSchemas() == rhsOperator->getInputSchemas()
            && getInputOriginIds() == rhsOperator->getInputOriginIds() && getOutputOriginIds() == rhsOperator->getOutputOriginIds()
            && getTraitSet() == rhsOperator->getTraitSet();
    }
    return false;
};

static std::string getFieldName(const LogicalFunction& function)
{
    /// We assert that the projection operator only contains field access and assignment functions in the constructor.
    if (const auto& fieldAccessLogicalFunction = function.tryGet<FieldAccessLogicalFunction>())
    {
        return fieldAccessLogicalFunction->getFieldName();
    }
    return function.get<FieldAssignmentLogicalFunction>().getField().getFieldName();
}

std::string ProjectionLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        if (not outputSchema.getFieldNames().empty())
        {
            return fmt::format("PROJECTION(opId: {}, schema: {})", id, outputSchema);
        }
        return fmt::format(
            "PROJECTION(opId: {}, fields: [{}])",
            id,
            fmt::join(std::views::transform(functions, [](const auto& function) { return getFieldName(function); }), ", "));
    }
    if (not outputSchema.getFieldNames().empty())
    {
        return fmt::format("PROJECTION(schema={})", outputSchema);
    }
    return fmt::format(
        "PROJECTION(fields: [{}])",
        fmt::join(std::views::transform(functions, [](const auto& function) { return getFieldName(function); }), ", "));
}

LogicalOperator ProjectionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(!inputSchemas.empty(), "Projection should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Projection operator");
        }
    }

    auto copy = *this;
    copy.inputSchema = firstSchema;
    copy.outputSchema = Schema{copy.outputSchema.memoryLayoutType};

    std::vector<LogicalFunction> newFunctions;
    for (const auto& function : functions)
    {
        auto func = function.withInferredDataType(firstSchema);

        if (auto fieldAccess = func.tryGet<FieldAccessLogicalFunction>())
        {
            copy.outputSchema.addField(fieldAccess->getFieldName(), fieldAccess->getDataType());
            newFunctions.emplace_back(*fieldAccess);
        }
        else if (func.tryGet<FieldAssignmentLogicalFunction>())
        {
            const auto& fieldAssignment = func.withInferredDataType(firstSchema).get<FieldAssignmentLogicalFunction>();
            copy.outputSchema.addField(fieldAssignment.getField().getFieldName(), fieldAssignment.getField().getDataType());
            newFunctions.emplace_back(fieldAssignment);
        }
        else
        {
            throw CannotInferSchema(
                "Function has to be a FieldAccessLogicalFunction or a "
                "FieldAssignmentLogicalFunction, but it was a {}",
                func);
        }
    }
    copy.functions = newFunctions;
    return copy;
}

TraitSet ProjectionLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator ProjectionLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

LogicalOperator ProjectionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> ProjectionLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema ProjectionLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> ProjectionLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> ProjectionLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator ProjectionLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Selection should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator ProjectionLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> ProjectionLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator ProjectionLogicalOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    const auto inputs = getInputSchemas();
    const auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i])
        {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds())
    {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    for (const auto& fn : getFunctions())
    {
        *funcList.add_functions() = fn.serialize();
    }
    (*serializableOperator.mutable_config())[ConfigParameters::PROJECTION_FUNCTION_NAME]
        = Configurations::descriptorConfigTypeToProto(funcList);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterProjectionLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto functionVariant = arguments.config[ProjectionLogicalOperator::ConfigParameters::PROJECTION_FUNCTION_NAME];

    if (std::holds_alternative<NES::FunctionList>(functionVariant))
    {
        const auto& functionList = std::get<FunctionList>(functionVariant);
        const auto& functions = functionList.functions();

        std::vector<LogicalFunction> functionVec;
        for (const auto& function : functions)
        {
            functionVec.emplace_back(FunctionSerializationUtil::deserializeFunction(function));
        }

        auto logicalOperator = ProjectionLogicalOperator(functionVec);
        if (auto& id = arguments.id)
        {
            logicalOperator.id = *id;
        }
        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);
    }
    throw UnknownLogicalOperator();
}

}
