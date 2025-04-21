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
#include <string>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

const ExpressionValue& LogicalMapOperator::getMapFunction() const
{
    return mapFunction;
}

bool LogicalMapOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalMapOperator>(rhs)->getId() == id;
}

bool LogicalMapOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalMapOperator>(rhs))
    {
        const auto mapOperator = NES::Util::as<LogicalMapOperator>(rhs);
        return mapFunction == mapOperator->mapFunction;
    }
    return false;
};

bool LogicalMapOperator::inferSchema()
{
    /// infer the default input and output schema
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }

    /// use the default input schema to calculate the out schema of this operator.
    mapFunction.inferStamp(getInputSchema());
    outputSchema.addField(asField, mapFunction.getStamp().value());

    return true;
}

std::string LogicalMapOperator::toString() const
{
    std::stringstream ss;
    ss << "MAP(opId: " << id << ": predicate: " << format_as(mapFunction) << ")";
    return ss.str();
}

std::shared_ptr<Operator> LogicalMapOperator::copy()
{
    auto copy = std::make_shared<LogicalMapOperator>(id, this->mapFunction, asField);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);

    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalMapOperator::inferStringSignature()
{
    NES_TRACE("LogicalMapOperator: Inferring String signature for {}", toString());
    INVARIANT(children.size() == 1, "Map should have 1 child, but got: {}", children.size());
    ///Infer query signatures for child operator
    const auto child = NES::Util::as<LogicalOperator>(children[0]);
    child->inferStringSignature();
    /// Infer signature for this operator.
    std::stringstream signatureStream;
    const auto childSignature = child->getHashBasedSignature();
    signatureStream << "MAP(" << format_as(mapFunction) << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}
