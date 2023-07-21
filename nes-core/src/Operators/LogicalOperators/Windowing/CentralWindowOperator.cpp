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
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>

#include <z3++.h>

namespace NES {

CentralWindowOperator::CentralWindowOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition, OperatorId id)
    : OperatorNode(id), WindowOperatorNode(windowDefinition, id) {
    windowDefinition->setDistributionCharacteristic(Windowing::DistributionCharacteristic::createCompleteWindowType());
}

std::string CentralWindowOperator::toString() const {
    std::stringstream ss;
    ss << "CENTRALWINDOW(" << id << ")";
    return ss.str();
}

bool CentralWindowOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<CentralWindowOperator>()->getId() == id;
}

bool CentralWindowOperator::equal(NodePtr const& rhs) const {
    return rhs->instanceOf<CentralWindowOperator>()
        && rhs->as<CentralWindowOperator>()->getWindowDefinition()->equal(this->getWindowDefinition());
}

OperatorNodePtr CentralWindowOperator::copy() {
    auto copy = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDefinition, id)->as<CentralWindowOperator>();
    copy->setOriginId(originId);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}
bool CentralWindowOperator::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {
    if (!WindowOperatorNode::inferSchema(typeInferencePhaseContext)) {
        return false;
    }
    // infer the default input and output schema
    NES_DEBUG("SliceCreationOperator: TypeInferencePhase: infer types for window operator with input schema {}",
              inputSchema->toString());

    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    for (auto& agg : windowAggregation) {
        agg->inferStamp(typeInferencePhaseContext, inputSchema);
    }

    if (windowDefinition->getWindowType()->isContentBasedWindowType()) {
        auto contentBasedWindowType = Windowing::WindowType::asContentBasedWindowType(windowDefinition->getWindowType());
        if (contentBasedWindowType->getContentBasedSubWindowType() == Windowing::ContentBasedWindowType::THRESHOLDWINDOW) {
            contentBasedWindowType->inferStamp(inputSchema, typeInferencePhaseContext);
        }
    } else {
        auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
        timeBasedWindowType->inferStamp(inputSchema, typeInferencePhaseContext);
    }

    //Construct output schema
    outputSchema->clear();
    /**
     * For the threshold window we cannot pre-calculate the window start and end, thus in the current version we ignores these output fields for
     * threshold windows
     */
    if (windowDefinition->getWindowType()->isTimeBasedWindowType()) {
        outputSchema = outputSchema
                           ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start",
                                                  BasicType::UINT64))
                           ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "end",
                                                  BasicType::UINT64));
    }
    if (windowDefinition->isKeyed()) {
        // infer the data type of the key field.
        auto keyList = windowDefinition->getKeys();
        for (auto& key : keyList) {
            key->inferStamp(typeInferencePhaseContext, inputSchema);
            outputSchema->addField(AttributeField::create(key->getFieldName(), key->getStamp()));
        }
    }

    for (auto& agg : windowAggregation) {
        NES_INFO("Add the following field to the output schema of the NonKeyedWindow {}",
                 agg->as()->as<FieldAccessExpressionNode>()->getFieldName());
        outputSchema->addField(
            AttributeField::create(agg->as()->as<FieldAccessExpressionNode>()->getFieldName(), agg->on()->getStamp()));
    }//end for
    NES_INFO("The final output schema is {}", outputSchema->toString())

    return true;
}

void CentralWindowOperator::inferStringSignature() { NES_NOT_IMPLEMENTED(); }
}// namespace NES
