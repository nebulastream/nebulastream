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

#include <algorithm>
#include <ranges>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentBinaryLogicalFunction.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

LogicalProjectionOperator::LogicalProjectionOperator(std::vector<std::shared_ptr<LogicalFunction>> functions, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), functions(std::move(functions))
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

const std::vector<std::shared_ptr<LogicalFunction>>& LogicalProjectionOperator::getFunctions() const
{
    return functions;
}

bool LogicalProjectionOperator::isIdentical(const std::shared_ptr<Operator>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalProjectionOperator>(rhs)->getId() == id;
}

bool LogicalProjectionOperator::equal(const std::shared_ptr<Operator>& rhs) const
{
    if (NES::Util::instanceOf<LogicalProjectionOperator>(rhs))
    {
        const auto projection = NES::Util::as<LogicalProjectionOperator>(rhs);
        return (*outputSchema == *projection->outputSchema);
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

std::string LogicalProjectionOperator::toString() const
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

bool LogicalProjectionOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
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
                "LogicalProjectionOperator: Function has to be an FieldAccessLogicalFunction, a "
                "RenameLogicalFunction, or a FieldAssignmentBinaryLogicalFunction, but it was a {}",
                *function));
        }
    }
    return true;
}

std::shared_ptr<Operator> LogicalProjectionOperator::clone() const
{
    auto copyOfProjectionFunctions
        = functions | std::views::transform([](const auto& fn) { return fn->deepCopy(); }) | ranges::to<std::vector>();
    auto copy = std::make_shared<LogicalProjectionOperator>(copyOfProjectionFunctions, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);


    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}
}
