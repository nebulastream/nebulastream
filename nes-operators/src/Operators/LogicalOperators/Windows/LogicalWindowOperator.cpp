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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
namespace NES {

LogicalWindowOperator::LogicalWindowOperator(const Windowing::LogicalWindowDescriptorPtr& windowDefinition, OperatorId id)
    : Operator(id), WindowOperator(windowDefinition, id) {}

std::string LogicalWindowOperator::toString() const {
    std::stringstream ss;
    auto windowType = windowDefinition->getWindowType();
    auto windowAggregation = windowDefinition->getWindowAggregation();
    ss << "WINDOW AGGREGATION(OP-" << id << ", ";
    for (auto agg : windowAggregation) {
        ss << agg->getTypeAsString() << ";";
    }
    ss << ")";
    return ss.str();
}

bool LogicalWindowOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && (rhs->as<LogicalWindowOperator>()->getId() == id);
}

bool LogicalWindowOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalWindowOperator>()) {
        auto rhsWindow = rhs->as<LogicalWindowOperator>();
        return windowDefinition->equal(rhsWindow->windowDefinition);
    }
    return false;
}

OperatorPtr LogicalWindowOperator::copy() {
    auto copy = LogicalOperatorFactory::createWindowOperator(windowDefinition, id)->as<LogicalWindowOperator>();
    copy->setOriginId(originId);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setStatisticId(statisticId);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalWindowOperator::inferSchema() {
    if (!WindowOperator::inferSchema()) {
        return false;
    }
    // infer the default input and output schema
    NES_DEBUG("LogicalWindowOperator: TypeInferencePhase: infer types for window operator with input schema {}",
              inputSchema->toString());

    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    for (const auto& agg : windowAggregation) {
        agg->inferStamp(inputSchema);
    }

    //Construct output schema
    //First clear()
    outputSchema->clear();
    // Distinguish process between different window types (currently time-based and content-based)
    auto windowType = windowDefinition->getWindowType();
    if (windowType->instanceOf<Windowing::TimeBasedWindowType>()) {
        // typeInference
        if (!windowType->as<Windowing::TimeBasedWindowType>()->inferStamp(inputSchema)) {
            return false;
        }
        outputSchema = outputSchema
                           ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start",
                                                  BasicType::UINT64))
                           ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "end",
                                                  BasicType::UINT64));
    } else if (windowType->instanceOf<Windowing::ContentBasedWindowType>()) {
        // type Inference for Content-based Windows requires the typeInferencePhaseContext
        auto contentBasedWindowType = windowType->as<Windowing::ContentBasedWindowType>();
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW) {
            auto thresholdWindow = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
            if (!thresholdWindow->inferStamp(inputSchema)) {
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported window type" << windowDefinition->getWindowType()->toString());
    }

    if (windowDefinition->isKeyed()) {

        // infer the data type of the key field.
        auto keyList = windowDefinition->getKeys();
        for (const auto& key : keyList) {
            key->inferStamp(inputSchema);
            outputSchema->addField(AttributeField::create(key->getFieldName(), key->getStamp()));
        }
    }
    for (const auto& agg : windowAggregation) {
        outputSchema->addField(
            AttributeField::create(agg->as()->as<FieldAccessExpressionNode>()->getFieldName(), agg->on()->getStamp()));
    }

    NES_DEBUG("Outputschema for window={}", outputSchema->toString());

    return true;
}

void LogicalWindowOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("Inferring String signature for {}", operatorNode->toString());

    //Infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto windowType = windowDefinition->getWindowType();
    auto windowAggregation = windowDefinition->getWindowAggregation();
    if (windowDefinition->isKeyed()) {
        signatureStream << "WINDOW-BY-KEY(";
        for (const auto& key : windowDefinition->getKeys()) {
            signatureStream << key->toString() << ",";
        }
    } else {
        signatureStream << "WINDOW(";
    }
    signatureStream << "WINDOW-TYPE: " << windowType->toString() << ",";
    signatureStream << "AGGREGATION: ";
    for (const auto& agg : windowAggregation) {
        signatureStream << agg->toString() << ",";
    }
    signatureStream << ")";
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << "." << *childSignature.begin()->second.begin();

    auto signature = signatureStream.str();
    //Update the signature
    auto hashCode = hashGenerator(signature);
    hashBasedSignature[hashCode] = {signature};
}

std::vector<std::string> LogicalWindowOperator::getGroupByKeyNames() const {
    std::vector<std::string> groupByKeyNames = {};
    auto windowDefinition = this->getWindowDefinition();
    if (windowDefinition->isKeyed()) {
        std::vector<FieldAccessExpressionNodePtr> groupByKeys = windowDefinition->getKeys();
        groupByKeyNames.reserve(groupByKeys.size());
        for (const auto& groupByKey : groupByKeys) {
            groupByKeyNames.push_back(groupByKey->getFieldName());
        }
    }
    return groupByKeyNames;
}

}// namespace NES
