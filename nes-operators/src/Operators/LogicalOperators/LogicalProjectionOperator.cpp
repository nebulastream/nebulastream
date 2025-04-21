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
#include <API/Schema.hpp>
#include <Functions/FieldAccessExpression.hpp>
#include <Functions/FieldAssignmentExpression.hpp>
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

LogicalProjectionOperator::LogicalProjectionOperator(std::vector<std::pair<Schema::Identifier, ExpressionValue>> functions, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), functions(std::move(functions))
{
}

LogicalProjectionOperator::LogicalProjectionOperator(std::vector<ExpressionValue> legacyExpressions, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), functions({})
{
    for (const auto& expression : legacyExpressions)
    {
        if (const auto* fieldAssignment = expression.as<FieldAssignmentExpression>())
        {
            functions.emplace_back(fieldAssignment->getField(), expression.getChildren().at(0));
        }
        else if (const auto* fieldAccess = expression.as<FieldAccessExpression>())
        {
            functions.emplace_back(fieldAccess->getFieldName(), expression.getChildren().at(0));
        }
        else
        {
            INVARIANT(false, "The Projection operator only supports NodeFunctionFieldAccess and NodeFunctionFieldAssignment functions.");
        }
    }
}

const std::vector<std::pair<Schema::Identifier, ExpressionValue>>& LogicalProjectionOperator::getFunctions() const
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
        const auto projection = NES::Util::as<LogicalProjectionOperator>(rhs);
        return (outputSchema == projection->outputSchema);
    }
    return false;
};

std::string LogicalProjectionOperator::toString() const
{
    PRECONDITION(not functions.empty(), "The projection operator must contain at least one function.");
    return fmt::format("PROJECTION(opId: {}, fields: [{}])", id, fmt::join(functions, ", "));
}

bool LogicalProjectionOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    NES_DEBUG("proj input={}  outputSchema={} this proj={}", inputSchema, outputSchema, toString());
    outputSchema = {};
    for (auto& [name, function] : functions)
    {
        ///Infer schema of the field function
        function.inferStamp(inputSchema);
        outputSchema.addField(name, function.getStamp().value());
    }
    return true;
}

std::shared_ptr<Operator> LogicalProjectionOperator::copy()
{
    auto copy = std::make_shared<LogicalProjectionOperator>(functions, id);
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
    std::sort(fields.begin(), fields.end());
    signatureStream << "PROJECTION(" << format_as(outputSchema) << ",";
    const auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}
