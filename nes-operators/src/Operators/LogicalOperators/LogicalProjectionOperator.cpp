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
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include "Nodes/Node.hpp"

namespace NES
{

LogicalProjectionOperator::LogicalProjectionOperator(std::vector<std::shared_ptr<NodeFunction>> functions, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), functions(std::move(functions))
{
    const auto functionTypeNotSupported = [](const std::shared_ptr<NodeFunction>& function) -> bool {
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
    if (NES::Util::instanceOf<LogicalProjectionOperator>(rhs))
    {
        auto projection = NES::Util::as<LogicalProjectionOperator>(rhs);
        return (*outputSchema == *projection->outputSchema);
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

std::string LogicalProjectionOperator::toString() const
{
    PRECONDITION(not functions.empty(), "The projection operator must contain at least one function.");
    std::stringstream ss;
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

        if (NES::Util::instanceOf<NodeFunctionFieldAccess>(function))
        {
            auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(function);
            outputSchema->addField(fieldAccess->getFieldName(), fieldAccess->getStamp());
        }
        else if (NES::Util::instanceOf<NodeFunctionFieldAssignment>(function))
        {
            auto fieldAssignment = NES::Util::as<NodeFunctionFieldAssignment>(function);
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
    std::vector<std::shared_ptr<NodeFunction>> copyOfProjectionFunctions;
    for (const auto& originalFunction : functions)
    {
        copyOfProjectionFunctions.emplace_back(originalFunction->deepCopy());
    }
    auto copy = std::make_shared<LogicalProjectionOperator>(copyOfProjectionFunctions, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
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
    std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalProjectionOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "LogicalProjectionOperator: Project should have children.");
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    std::vector<std::string> fields;
    for (const auto& field : *outputSchema)
    {
        fields.push_back(field->getName());
    }
    std::sort(fields.begin(), fields.end());
    signatureStream << "PROJECTION(";
    for (const auto& field : fields)
    {
        signatureStream << " " << field << " ";
    }
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}
