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
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <numeric>
#include <utility>

namespace NES::Statistic {

LogicalStatisticWindowOperator::LogicalStatisticWindowOperator(OperatorId id,
                                                               Windowing::WindowTypePtr windowType,
                                                               WindowStatisticDescriptorPtr windowStatisticDescriptor,
                                                               MetricHash metricHash)
    : Operator(id), LogicalUnaryOperator(id), windowType(std::move(windowType)),
      windowStatisticDescriptor(std::move(windowStatisticDescriptor)), metricHash(metricHash) {}

bool LogicalStatisticWindowOperator::inferSchema() {
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }
    const auto qualifierNameWithSeparator = inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator();
    outputSchema->clear();
    outputSchema->addField(qualifierNameWithSeparator + BASE_FIELD_NAME_START, BasicType::UINT64);
    outputSchema->addField(qualifierNameWithSeparator + BASE_FIELD_NAME_END, BasicType::UINT64);
    outputSchema->addField(qualifierNameWithSeparator + STATISTIC_HASH_FIELD_NAME, BasicType::UINT64);
    outputSchema->addField(qualifierNameWithSeparator + STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64);
    outputSchema->addField(qualifierNameWithSeparator + OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64);
    windowStatisticDescriptor->addDescriptorFields(*outputSchema, qualifierNameWithSeparator);

    NES_DEBUG("OutputSchema is = {}", outputSchema->toString());

    return true;
}

bool LogicalStatisticWindowOperator::equal(const NodePtr& rhs) const {
    if (rhs->instanceOf<LogicalStatisticWindowOperator>()) {
        auto rhsStatisticOperatorNode = rhs->as<LogicalStatisticWindowOperator>();
        return windowType->equal(rhsStatisticOperatorNode->windowType)
            && inputOriginIds == rhsStatisticOperatorNode->inputOriginIds
            && windowStatisticDescriptor->equal(rhsStatisticOperatorNode->windowStatisticDescriptor)
            && metricHash == rhsStatisticOperatorNode->metricHash;
    }
    return false;
}

bool LogicalStatisticWindowOperator::isIdentical(const NodePtr& rhs) const {
    return equal(rhs) && rhs->as<LogicalStatisticWindowOperator>()->getId() == id;
}

std::string LogicalStatisticWindowOperator::toString() const {
    std::ostringstream oss;
    oss << "Windowtype: " << windowType->toString() << std::endl;
    oss << "Descriptor: " << windowStatisticDescriptor->toString() << std::endl;
    oss << "InputOriginIds: " << std::accumulate(inputOriginIds.begin(), inputOriginIds.end(), std::string(),
                                                 [] (const std::string& str, const OriginId id) {
                                                     if (str.empty()) {
                                                         return std::to_string(id);
                                                     } else {
                                                         return str + ", " + std::to_string(id);
                                                     }
                                                 });
    oss << "MetricHash: " << metricHash;

    return oss.str();
}

OperatorPtr LogicalStatisticWindowOperator::copy() {
    auto copy = LogicalOperatorFactory::createStatisticBuildOperator(windowType, windowStatisticDescriptor, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

Windowing::WindowTypePtr LogicalStatisticWindowOperator::getWindowType() const { return windowType; }

WindowStatisticDescriptorPtr LogicalStatisticWindowOperator::getWindowStatisticDescriptor() const {
    return windowStatisticDescriptor;
}

void LogicalStatisticWindowOperator::inferStringSignature() {
    auto op = shared_from_this()->as<Operator>();
    NES_TRACE("StatisticWindowOperatorNode: Inferring String signature for {}", op->toString());

    std::stringstream signatureStream;
    signatureStream << "STATISTIC_BUILD_OPERATOR(" + windowStatisticDescriptor->toString() + ").";

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}// namespace NES::Statistic