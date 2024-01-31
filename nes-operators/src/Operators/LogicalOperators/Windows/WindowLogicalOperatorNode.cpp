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
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Types/ContentBasedWindowType.hpp>
#include <Operators/LogicalOperators/Windows/Types/ThresholdWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <Operators/LogicalOperators/Windows/WindowLogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
namespace NES {

WindowLogicalOperatorNode::WindowLogicalOperatorNode(const Windowing::LogicalWindowDefinitionPtr& windowDefinition, OperatorId id)
    : OperatorNode(id), WindowOperatorNode(windowDefinition, id) {}

std::string WindowLogicalOperatorNode::toString() const {
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

bool WindowLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && (rhs->as<WindowLogicalOperatorNode>()->getId() == id);
}

bool WindowLogicalOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<WindowLogicalOperatorNode>()) {
        auto rhsWindow = rhs->as<WindowLogicalOperatorNode>();
        return windowDefinition->equal(rhsWindow->windowDefinition);
    }
    return false;
}

OperatorNodePtr WindowLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createWindowOperator(windowDefinition, id)->as<WindowLogicalOperatorNode>();
    copy->setOriginId(originId);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool WindowLogicalOperatorNode::inferSchema() {
    if (!WindowOperatorNode::inferSchema()) {
        return false;
    }
    // infer the default input and output schema
    NES_DEBUG("WindowLogicalOperatorNode: TypeInferencePhase: infer types for window operator with input schema {}",
              inputSchema->toString());

    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    for (auto& agg : windowAggregation) {
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
        if (contentBasedWindowType->getContentBasedSubWindowType() == Windowing::ContentBasedWindowType::THRESHOLDWINDOW) {
            auto thresholdWindow = contentBasedWindowType->asThresholdWindow(contentBasedWindowType);
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
        for (auto& key : keyList) {
            key->inferStamp(inputSchema);
            outputSchema->addField(AttributeField::create(key->getFieldName(), key->getStamp()));
        }
    }
    for (auto& agg : windowAggregation) {
        outputSchema->addField(
            AttributeField::create(agg->as()->as<FieldAccessExpressionNode>()->getFieldName(), agg->on()->getStamp()));
    }

    NES_DEBUG("Outputschema for window={}", outputSchema->toString());

    return true;
}

void WindowLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("Inferring String signature for {}", operatorNode->toString());

    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto windowType = windowDefinition->getWindowType();
    auto windowAggregation = windowDefinition->getWindowAggregation();
    if (windowDefinition->isKeyed()) {
        signatureStream << "WINDOW-BY-KEY(";
        for (auto& key : windowDefinition->getKeys()) {
            signatureStream << key->toString() << ",";
        }
    } else {
        signatureStream << "WINDOW(";
    }
    signatureStream << "WINDOW-TYPE: " << windowType->toString() << ",";
    signatureStream << "AGGREGATION: ";
    for (auto& agg : windowAggregation) {
        signatureStream << agg->toString() << ",";
    }
    signatureStream << ")";
    auto childSignature = children[0]->as<LogicalOperatorNode>()->getHashBasedSignature();
    signatureStream << "." << *childSignature.begin()->second.begin();

    auto signature = signatureStream.str();
    //Update the signature
    auto hashCode = hashGenerator(signature);
    hashBasedSignature[hashCode] = {signature};
}

std::vector<std::string> WindowLogicalOperatorNode::getGroupByKeyNames() {
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
