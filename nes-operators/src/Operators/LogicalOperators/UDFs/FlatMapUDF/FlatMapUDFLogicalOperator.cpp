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

#include <any>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <unordered_map>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperator.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <__fwd/sstream.h>

#include <Identifiers/NESStrongType.hpp>
#include <Operators/LogicalOperators/UDFs/UDFLogicalOperator.hpp>
#include <Operators/Operator.hpp>

namespace NES
{

FlatMapUDFLogicalOperator::FlatMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor, OperatorId id)
    : Operator(id), UDFLogicalOperator(udfDescriptor, id)
{
}

std::string FlatMapUDFLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "FLATMAP_UDF(" << id << ")";
    return ss.str();
}

OperatorPtr FlatMapUDFLogicalOperator::copy()
{
    auto copy = std::make_shared<FlatMapUDFLogicalOperator>(this->getUDFDescriptor(), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

bool FlatMapUDFLogicalOperator::equal(const NodePtr& other) const
{
    return other->instanceOf<FlatMapUDFLogicalOperator>() && UDFLogicalOperator::equal(other);
}

bool FlatMapUDFLogicalOperator::isIdentical(const NodePtr& other) const
{
    return equal(other) && id == other->as<FlatMapUDFLogicalOperator>()->id;
}

} /// namespace NES
