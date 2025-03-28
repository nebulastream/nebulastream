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

#include <ranges>
#include <utility>
#include <API/AttributeField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentBinaryLogicalFunction.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Operators/LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

ProjectionLogicalOperator::ProjectionLogicalOperator(std::vector<std::shared_ptr<LogicalFunction>> functions, OperatorId id)
    : Operator(id), UnaryLogicalOperator(id), functions(std::move(functions))
{
    const auto functionTypeNotSupported = [](const std::shared_ptr<LogicalFunction>& function) -> bool
    {
        return not(
            NES::Util::instanceOf<FieldAccessLogicalFunction>(function)
            or NES::Util::instanceOf<FieldAssignmentBinaryLogicalFunction>(function));
    };
    bool allFunctionsAreSupported = true;
    for (const auto& function : this->functions | std::views::filter(functionTypeNotSupported))
    {
        NES_ERROR("The projection operator does not support the function: {}", *function);
        allFunctionsAreSupported = false;
    }
    INVARIANT(
        allFunctionsAreSupported,
        "The Projection operator only supports FieldAccessLogicalFunction and FieldAssignmentBinaryLogicalFunction functions.");
}

const std::vector<std::shared_ptr<LogicalFunction>>& ProjectionLogicalOperator::getFunctions() const
{
    return functions;
}

bool ProjectionLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const ProjectionLogicalOperator*>(&rhs)->getId() == id;
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
    return dynamic_cast<FieldAssignmentBinaryLogicalFunction const*>(&function)->getField()->getFieldName();
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
        else if (NES::Util::instanceOf<FieldAssignmentBinaryLogicalFunction>(function))
        {
            const auto fieldAssignment = NES::Util::as<FieldAssignmentBinaryLogicalFunction>(function);
            outputSchema->addField(fieldAssignment->getField()->getFieldName(), fieldAssignment->getField()->getStamp());
        }
        else
        {
            throw CannotInferSchema(fmt::format(
                "ProjectionLogicalOperator: Function has to be an FieldAccessLogicalFunction, a "
                "RenameLogicalFunction, or a FieldAssignmentBinaryLogicalFunction, but it was a {}",
                *function));
        }
    }
    return true;
}

std::shared_ptr<Operator> ProjectionLogicalOperator::clone() const
{
    auto copyOfProjectionFunctions
        = functions | std::views::transform([](const auto& fn) { return fn->deepCopy(); }) | ranges::to<std::vector>();
    auto copy = std::make_shared<ProjectionLogicalOperator>(copyOfProjectionFunctions, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

std::unique_ptr<LogicalOperator> LogicalOperatorGeneratedRegistrar::deserializeProjectionLogicalOperator(const SerializableOperator& serializableOperator)
{
    auto details = serializableOperator.details();
    std::shared_ptr<LogicalOperator> operatorNode;
    auto projectionDetails = SerializableOperator_ProjectionDetails();
    details.UnpackTo(&projectionDetails);
    std::vector<std::shared_ptr<LogicalFunction>> functions;
    for (const auto& serializedFunction : projectionDetails.function())
    {
        auto projectFunction = FunctionSerializationUtil::deserializeFunction(serializedFunction);
        functions.push_back(projectFunction);
    }
    return std::make_unique<ProjectionLogicalOperator>(functions, getNextOperatorId());
}

}
