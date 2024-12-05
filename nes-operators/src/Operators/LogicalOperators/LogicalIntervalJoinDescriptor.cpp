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
#include <Operators/LogicalOperators/LogicalIntervalJoinDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
namespace NES::Join {

LogicalIntervalJoinDescriptor::LogicalIntervalJoinDescriptor(ExpressionNodePtr joinExpression,
                                                             Windowing::TimeCharacteristicPtr timeCharacteristic,
                                                             int64_t lowerBound,
                                                             int64_t upperBound)
    : joinExpression(joinExpression), timeCharacteristic(timeCharacteristic), lowerBound(lowerBound), upperBound(upperBound) {

    NES_ASSERT(this->joinExpression, "Invalid join expression");
}

LogicalIntervalJoinDescriptorPtr LogicalIntervalJoinDescriptor::create(ExpressionNodePtr joinExpression,
                                                                       Windowing::TimeCharacteristicPtr timeCharacteristic,
                                                                       int64_t lowerBound,
                                                                       int64_t upperBound) {
    return std::make_shared<Join::LogicalIntervalJoinDescriptor>(joinExpression, timeCharacteristic, lowerBound, upperBound);
}

ExpressionNodePtr LogicalIntervalJoinDescriptor::getJoinExpression() const { return joinExpression; }

SchemaPtr LogicalIntervalJoinDescriptor::getLeftSchema() const { return leftSchema; }

SchemaPtr LogicalIntervalJoinDescriptor::getRightSchema() const { return rightSchema; }

Windowing::TimeCharacteristicPtr LogicalIntervalJoinDescriptor::getTimeCharacteristic() const { return timeCharacteristic; };

int64_t LogicalIntervalJoinDescriptor::getLowerBound() const { return lowerBound; }

int64_t LogicalIntervalJoinDescriptor::getUpperBound() const { return upperBound; }

void LogicalIntervalJoinDescriptor::updateInputSchemas(SchemaPtr leftSchema, SchemaPtr rightSchema) {
    this->leftSchema = std::move(leftSchema);
    this->rightSchema = std::move(rightSchema);
}

void LogicalIntervalJoinDescriptor::updateOutputDefinition(SchemaPtr outputSchema) {
    this->outputSchema = std::move(outputSchema);
}

SchemaPtr LogicalIntervalJoinDescriptor::getOutputSchema() const { return outputSchema; }

std::string LogicalIntervalJoinDescriptor::toString() const {
    std::stringstream ss;

    ss << "INTERVALJOINDEFINITION(joinExpression: " << joinExpression->toString() << "; ";
    if (leftSchema != nullptr) {
        ss << "leftSchema: " << leftSchema->toString() << "; ";
    } else {
        ss << "leftSchema: null; ";
    }
    if (rightSchema != nullptr) {
        ss << "rightSchema: " << rightSchema->toString() << "; ";
    } else {
        ss << "rightSchema: null; ";
    }
    if (outputSchema != nullptr) {
        ss << "outputSchema: " << outputSchema->toString() << "; ";
    } else {
        ss << "outputSchema: null; ";
    }
    ss << "timeCharacteristic: " << timeCharacteristic->toString() << "; ";
    ss << "lowerBound: " << lowerBound << "; ";
    ss << "upperBound: " << upperBound << ")";
    return ss.str();
}

void LogicalIntervalJoinDescriptor::updateIntervalFields(std::string intervalStartFieldName, std::string intervalEndFieldName) {
    this->intervalEndFieldName = intervalEndFieldName;
    this->intervalStartFieldName = intervalStartFieldName;
}

const std::string& LogicalIntervalJoinDescriptor::getIntervalStartFieldName() const { return intervalStartFieldName; }

const std::string& LogicalIntervalJoinDescriptor::getIntervalEndFieldName() const { return intervalEndFieldName; }

};// namespace NES::Join
