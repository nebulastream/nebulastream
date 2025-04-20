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
#include <ranges>
#include <utility>
#include <variant>
#include <memory>
#include <vector>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

ProjectionLogicalOperator::ProjectionLogicalOperator(std::vector<LogicalFunction> functions) : functions(functions)
{
    const auto functionTypeNotSupported = [](const LogicalFunction& function) -> bool
    {
        return not(function.tryGet<FieldAccessLogicalFunction>() or function.tryGet<FieldAssignmentLogicalFunction>());
    };
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

bool ProjectionLogicalOperator::operator==(LogicalOperatorConcept const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const ProjectionLogicalOperator*>(&rhs)) {
        return (getOutputSchema() == rhsOperator->getOutputSchema());
    }
    return false;
};

std::string getFieldName(const LogicalFunction& function)
{
    /// We assert that the projection operator only contains field access and assignment functions in the constructor.
    if (const auto& fieldAccessLogicalFunction = function.tryGet<FieldAccessLogicalFunction>())
    {
        return fieldAccessLogicalFunction->getFieldName();
    }
    return function.get<FieldAssignmentLogicalFunction>().getField().getFieldName();
}

std::string ProjectionLogicalOperator::toString() const
{
    PRECONDITION(not functions.empty(), "The projection operator must contain at least one function.");
    if (not getOutputSchema().getFieldNames().empty())
    {
        return fmt::format("PROJECTION(opId: {}, schema={})", id, getOutputSchema().toString());
    }
    return fmt::format(
        "PROJECTION(opId: {}, fields: [{}])",
        id,
        fmt::join(std::views::transform(functions, [](const auto& function) { return getFieldName(function); }), ", "));
}


LogicalOperator ProjectionLogicalOperator::withInferredSchema(Schema inputSchema) const
{
    auto copy = *this;
    copy.inputSchema = inputSchema;
    copy.outputSchema.clear();

    std::vector<LogicalFunction> newFunctions;
    for (auto& function : functions)
    {
        if (function.tryGet<FieldAccessLogicalFunction>())
        {
            const auto& fieldAccess = function.get<FieldAccessLogicalFunction>();
            copy.outputSchema.addField(fieldAccess.getFieldName(), fieldAccess.getStamp());
            std::cout << "Projection access: " << fieldAccess.getFieldName() << "\n";
            std::cout << "Projection access: " << fieldAccess.getStamp()->toString() << "\n";
            newFunctions.emplace_back(fieldAccess);
        }
        else if (function.tryGet<FieldAssignmentLogicalFunction>())
        {
            const auto& fieldAssignment = function.withInferredStamp(inputSchema).get<FieldAssignmentLogicalFunction>();
            auto stampPtr = fieldAssignment.getField().getStamp();
            copy.outputSchema = copy.outputSchema.addField(fieldAssignment.getField().getFieldName(), fieldAssignment.getField().getStamp());
            newFunctions.emplace_back(fieldAssignment);
        }
        else
        {
            throw CannotInferSchema(
                "Function has to be a FieldAccessLogicalFunction or a FieldAssignmentLogicalFunction, but it was a {}",
                function);
        }
    }
    copy.functions = newFunctions;

    std::vector<LogicalOperator> newChildren;
    for (auto& child : children)
    {
        newChildren.push_back(child.withInferredSchema(copy.outputSchema));
    }
    return copy.withChildren(newChildren);
}

Optimizer::TraitSet ProjectionLogicalOperator::getTraitSet() const
{
    return {};
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
    return inputOriginIds;
}

std::vector<OriginId> ProjectionLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

void ProjectionLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void ProjectionLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids;
}

std::vector<LogicalOperator> ProjectionLogicalOperator::getChildren() const
{
    return children;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterProjectionLogicalOperator(NES::LogicalOperatorRegistryArguments config)
{
    auto functionVariant = config.config[ProjectionLogicalOperator::ConfigParameters::PROJECTION_FUNCTION_NAME];

    if (std::holds_alternative<NES::FunctionList>(functionVariant)) {
        const auto& functionList = std::get<FunctionList>(functionVariant);
        const auto& functions = functionList.functions();

        std::vector<LogicalFunction> functionVec;
        for (const auto &function : functions)
        {
            auto functionType = function.functiontype();
            NES::Configurations::DescriptorConfig::Config functionDescriptorConfig{};
            for (const auto& [key, value] : function.config())
            {
                functionDescriptorConfig[key] = Configurations::protoToDescriptorConfigType(value);
            }

            if (auto function = LogicalFunctionRegistry::instance().create(functionType, LogicalFunctionRegistryArguments(functionDescriptorConfig)))
            {
                functionVec.emplace_back(std::move(function.value()));
            }
        }
        return ProjectionLogicalOperator(functionVec);
    }
    throw UnknownLogicalOperator();
}


SerializableOperator ProjectionLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(getChildren()[0].getId().getRawValue());

    NES::FunctionList list;
    for (const auto& function : this->getFunctions())
    {
        auto* serializedFunction = list.add_functions();
        serializedFunction->CopyFrom(function.serialize());
    }

    NES::Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor =
        Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["selectionFunctionName"] = variantDescriptor;

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();
    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchemas()[0], inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    const auto ids = this->getInputOriginIds()[0];
    for (const auto& originId : ids) {
        unaryOpDesc->add_originids(originId.getRawValue());
    }

    opDesc->set_allocated_unaryoperator(unaryOpDesc);
    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getOutputSchema(), outputSchema);

    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

}
