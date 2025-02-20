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
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

ProjectionLogicalOperator::ProjectionLogicalOperator(std::vector<std::shared_ptr<LogicalFunction>> functions)
    : Operator(), UnaryLogicalOperator(), functions(std::move(functions))
{
    const auto functionTypeNotSupported = [](const std::shared_ptr<LogicalFunction>& function) -> bool
    {
        return not(
            NES::Util::instanceOf<FieldAccessLogicalFunction>(function)
            or NES::Util::instanceOf<FieldAssignmentLogicalFunction>(function));
    };
    bool allFunctionsAreSupported = true;
    for (const auto& function : this->functions | std::views::filter(functionTypeNotSupported))
    {
        NES_ERROR("The projection operator does not support the function: {}", *function);
        allFunctionsAreSupported = false;
    }
    INVARIANT(
        allFunctionsAreSupported,
        "The Projection operator only supports FieldAccessLogicalFunction and FieldAssignmentLogicalFunction functions.");
}

const std::vector<std::shared_ptr<LogicalFunction>>& ProjectionLogicalOperator::getFunctions() const
{
    return functions;
}

bool ProjectionLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const ProjectionLogicalOperator*>(&rhs)->id == id;
}

bool ProjectionLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const ProjectionLogicalOperator*>(&rhs)) {
        return (*outputSchema == *rhsOperator->outputSchema);
    }
    return false;
};

std::string getFieldName(const LogicalFunction& function)
{
    /// We assert that the projection operator only contains field access and assignment functions in the constructor.
    if (const auto *FieldAccessLogicalFunction = dynamic_cast<class FieldAccessLogicalFunction const*>(&function))
    {
        return FieldAccessLogicalFunction->getFieldName();
    }
    return dynamic_cast<FieldAssignmentLogicalFunction const*>(&function)->getField()->getFieldName();
}

std::string ProjectionLogicalOperator::toString() const
{
    PRECONDITION(not functions.empty(), "The projection operator must contain at least one function.");
    if (not outputSchema->getFieldNames().empty())
    {
        return fmt::format("PROJECTION(opId: {}, schema={})", id, outputSchema->toString());
    }
    return fmt::format(
        "PROJECTION(opId: {}, fields: [{}])",
        id,
        fmt::join(std::views::transform(functions, [](const auto& function) { return getFieldName(*function); }), ", "));
}

bool ProjectionLogicalOperator::inferSchema()
{
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }
    NES_DEBUG("proj input={}  outputSchema={} this proj={}", inputSchema->toString(), outputSchema->toString(), toString());
    outputSchema->clear();
    for (const auto& function : functions)
    {
        ///Infer schema of the field function
        function->inferStamp(*inputSchema);

        if (NES::Util::instanceOf<FieldAccessLogicalFunction>(function))
        {
            const auto fieldAccess = NES::Util::as<FieldAccessLogicalFunction>(function);
            outputSchema->addField(fieldAccess->getFieldName(), fieldAccess->getStamp());
        }
        else if (NES::Util::instanceOf<FieldAssignmentLogicalFunction>(function))
        {
            const auto fieldAssignment = NES::Util::as<FieldAssignmentLogicalFunction>(function);
            outputSchema->addField(fieldAssignment->getField()->getFieldName(), fieldAssignment->getField()->getStamp());
        }
        else
        {
            throw CannotInferSchema(fmt::format(
                "ProjectionLogicalOperator: Function has to be an FieldAccessLogicalFunction, a "
                "RenameLogicalFunction, or a FieldAssignmentLogicalFunction, but it was a {}",
                *function));
        }
    }
    return true;
}

std::shared_ptr<Operator> ProjectionLogicalOperator::clone() const
{
    auto copyOfProjectionFunctions
        = functions | std::views::transform([](const auto& fn) { return fn->clone(); }) | std::ranges::to<std::vector>();
    auto copy = std::make_shared<ProjectionLogicalOperator>(copyOfProjectionFunctions);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

std::unique_ptr<LogicalOperator>
LogicalOperatorGeneratedRegistrar::RegisterProjectionLogicalOperator(NES::LogicalOperatorRegistryArguments config)
{
    auto functionVariant = config.config[ProjectionLogicalOperator::ConfigParameters::PROJECTION_FUNCTION_NAME];

    if (std::holds_alternative<NES::FunctionList>(functionVariant)) {
        const auto& functionList = std::get<FunctionList>(functionVariant);
        const auto& functions = functionList.functions();

        std::vector<std::shared_ptr<LogicalFunction>> functionVec(functions.size());
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
                std::shared_ptr<NES::LogicalFunction> sharedBase(function.value().get());
                functionVec.emplace_back(sharedBase);
            }
            else
            {
                return nullptr;
            }
        }
        return std::make_unique<ProjectionLogicalOperator>(functionVec);
    }
    return nullptr;
}


SerializableOperator ProjectionLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0]->id.getRawValue());

    NES::FunctionList list;
    for (const auto& function : this->getFunctions())
    {
        auto* serializedFunction = list.add_functions();
        serializedFunction->CopyFrom(function->serialize());
    }

    NES::Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor =
        Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["selectionFunctionName"] = variantDescriptor;

    auto* unaryOpDesc = new SerializableOperator_UnaryLogicalOperator();
    auto* inputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchema(), inputSchema);
    unaryOpDesc->set_allocated_inputschema(inputSchema);

    for (const auto& originId : this->getInputOriginIds()) {
        unaryOpDesc->add_originids(originId.getRawValue());
    }

    opDesc->set_allocated_unaryoperator(unaryOpDesc);
    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->outputSchema, outputSchema);

    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

}
