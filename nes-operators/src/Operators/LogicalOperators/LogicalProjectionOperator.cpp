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
#include <cstddef>
#include <ostream>
#include <ranges>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
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

LogicalProjectionOperator::LogicalProjectionOperator(std::vector<std::shared_ptr<NodeFunction>> functions, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), functions(std::move(functions))
{
    const auto functionTypeNotSupported = [](const std::shared_ptr<NodeFunction>& function) -> bool
    {
        return not(
            NES::Util::instanceOf<NodeFunctionFieldAccess>(function) or NES::Util::instanceOf<NodeFunctionFieldAssignment>(function));
    };
    bool allFunctionsAreSupported = true;
    for (const auto& function : this->functions | std::views::filter(functionTypeNotSupported))
    {
        NES_ERROR("The projection operator does not support the function: {}", *function);
        allFunctionsAreSupported = false;
    }
    INVARIANT(
        allFunctionsAreSupported,
        "The Projection operator only supports NodeFunctionFieldAccess and NodeFunctionFieldAssignment functions.");
}

const std::vector<std::shared_ptr<NodeFunction>>& LogicalProjectionOperator::getFunctions() const
{
    return functions;
}

bool LogicalProjectionOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalProjectionOperator>(rhs)->getId() == id;
}

bool LogicalProjectionOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (const auto projection = NES::Util::as_if<LogicalProjectionOperator>(rhs);
        (projection != nullptr) and (this->functions.size() == projection->getFunctions().size()))
    {
        for (size_t i = 0; const auto& function : this->functions)
        {
            if (not function->equal(projection->getFunctions()[i++]))
            {
                return false;
            }
        }
        return *outputSchema == *projection->outputSchema;
    }
    return false;
};

std::string getFieldName(const NodeFunction& function)
{
    /// We assert that the projection operator only contains field access and assignment functions in the constructor.
    if (const auto* nodeFunctionFieldAccess = dynamic_cast<const NodeFunctionFieldAccess*>(&function))
    {
        return nodeFunctionFieldAccess->getFieldName();
    }
    return dynamic_cast<const NodeFunctionFieldAssignment*>(&function)->getField()->getFieldName();
}

std::ostream& LogicalProjectionOperator::toDebugString(std::ostream& os) const
{
    PRECONDITION(not functions.empty(), "The projection operator must contain at least one function.");
    if (not outputSchema->getFieldNames().empty())
    {
        return os << fmt::format("PROJECTION(opId: {}, schema={})", id, outputSchema->toString());
    }
    return os << fmt::format(
               "PROJECTION(opId: {}, fields: [{}])",
               id,
               fmt::join(std::views::transform(functions, [](const auto& function) { return getFieldName(*function); }), ", "));
}

std::ostream& LogicalProjectionOperator::toQueryPlanString(std::ostream& os) const
{
    PRECONDITION(not functions.empty(), "The projection operator must contain at least one function.");
    if (not outputSchema->getFieldNames().empty())
    {
        return os << fmt::format("PROJECTION(schema={})", outputSchema->toString());
    }
    return os << fmt::format(
               "PROJECTION(fields: [{}])",
               fmt::join(std::views::transform(functions, [](const auto& function) { return getFieldName(*function); }), ", "));
}

bool LogicalProjectionOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    NES_DEBUG("proj input={}  outputSchema={} this proj={}", inputSchema->toString(), outputSchema->toString(), *this);
    outputSchema->clear();
    for (const auto& function : functions)
    {
        ///Infer schema of the field function
        function->inferStamp(*inputSchema);

        if (NES::Util::instanceOf<NodeFunctionFieldAccess>(function))
        {
            const auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(function);
            outputSchema->addField(fieldAccess->getFieldName(), fieldAccess->getStamp());
        }
        else if (NES::Util::instanceOf<NodeFunctionFieldAssignment>(function))
        {
            const auto fieldAssignment = NES::Util::as<NodeFunctionFieldAssignment>(function);
            outputSchema->addField(fieldAssignment->getField()->getFieldName(), fieldAssignment->getField()->getStamp());
        }
        else
        {
            throw CannotInferSchema(fmt::format(
                "LogicalProjectionOperator: Function has to be an NodeFunctionFieldAccess, a "
                "NodeFunctionFieldRename, or a NodeFunctionFieldAssignment, but it was a {}",
                *function));
        }
    }
    return true;
}

std::shared_ptr<Operator> LogicalProjectionOperator::copy()
{
    auto copyOfProjectionFunctions
        = functions | std::views::transform([](const auto& fn) { return fn->deepCopy(); }) | std::ranges::to<std::vector>();
    auto copy = std::make_shared<LogicalProjectionOperator>(copyOfProjectionFunctions, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setOperatorState(operatorState);
    copy->setHashBasedSignature(hashBasedSignature);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalProjectionOperator::inferStringSignature()
{
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalProjectionOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "LogicalProjectionOperator: Project should have children.");
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    std::vector<std::string> fields;
    for (const auto& field : *outputSchema)
    {
        fields.push_back(field->getName());
    }
    std::ranges::sort(fields);
    signatureStream << "PROJECTION(";
    for (const auto& field : fields)
    {
        signatureStream << " " << field << " ";
    }
    const auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}
