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
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Functions/FieldRenameFunctionNode.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{

LogicalProjectionOperator::LogicalProjectionOperator(std::vector<FunctionNodePtr> functions, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), functions(std::move(functions))
{
}

std::vector<FunctionNodePtr> LogicalProjectionOperator::getFunctions() const
{
    return functions;
}

bool LogicalProjectionOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalProjectionOperator>(rhs)->getId() == id;
}

bool LogicalProjectionOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LogicalProjectionOperator>(rhs))
    {
        auto projection = NES::Util::as<LogicalProjectionOperator>(rhs);
        return outputSchema->equals(projection->outputSchema);
    }
    return false;
};

std::string LogicalProjectionOperator::toString() const
{
    std::stringstream ss;
    ss << "PROJECTION(" << id << ", schema=" << outputSchema->toString() << ")";
    return ss.str();
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
        function->inferStamp(inputSchema);

        /// Build the output schema
        if (NES::Util::instanceOf<FieldRenameFunctionNode>(function))
        {
            auto fieldRename = NES::Util::as<FieldRenameFunctionNode>(function);
            outputSchema->addField(fieldRename->getNewFieldName(), fieldRename->getStamp());
        }
        else if (NES::Util::instanceOf<FieldAccessFunctionNode>(function))
        {
            auto fieldAccess = NES::Util::as<FieldAccessFunctionNode>(function);
            outputSchema->addField(fieldAccess->getFieldName(), fieldAccess->getStamp());
        }
        else
        {
            NES_ERROR(
                "LogicalProjectionOperator: Function has to be an FieldAccessFunction or a FieldRenameFunction "
                "but it was a {}",
                function->toString());
            throw TypeInferenceException(
                "LogicalProjectionOperator: Function has to be an FieldAccessFunction or a "
                "FieldRenameFunction but it was a "
                + function->toString());
        }
    }
    return true;
}

OperatorPtr LogicalProjectionOperator::copy()
{
    std::vector<FunctionNodePtr> copyOfProjectionFunctions;
    for (const auto& originalFunction : functions)
    {
        copyOfProjectionFunctions.emplace_back(originalFunction->deepCopy());
    }
    auto copy = LogicalOperatorFactory::createProjectionOperator(copyOfProjectionFunctions, id);
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
    OperatorPtr operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalProjectionOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "LogicalProjectionOperator: Project should have children.");
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    std::vector<std::string> fields;
    for (const auto& field : outputSchema->fields)
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
} /// namespace NES
