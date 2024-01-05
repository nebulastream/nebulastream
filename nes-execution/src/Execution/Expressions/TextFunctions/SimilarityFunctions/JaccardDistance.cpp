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
#include <Execution/Expressions/TextFunctions/SimilarityFunctions/JaccardDistance.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <regex>
#include <string>

namespace NES::Runtime::Execution::Expressions {

JaccardDistance::JaccardDistance(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                                 const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), rightSubExpression(rightSubExpression) {}

/**
  * @brief Compares two text object and returns their Jaccard Distance as Double.
  * @param leftText text object
  * @param rightText text object
  * @return Jaccard Distance as Integer.
  */
double textJaccard(const TextValue* leftText, const TextValue* rightText) {
    std::string unionSet;
    std::string intersectionSet;
    /** calculate intersection and union, then return the ratio of their lengths */
    for (int32_t i = 0; i < (int32_t) leftText->length(); i++) {
        if (unionSet.find(leftText->c_str()[i]) == std::string::npos) {
            unionSet.push_back(leftText->c_str()[i]);
        }

        const char* pos = std::find(rightText->c_str(), rightText->c_str() + rightText->length(), leftText->c_str()[i]);
        if (std::string::npos == intersectionSet.find(leftText->c_str()[i]) && pos != rightText->c_str() + rightText->length()) {
            intersectionSet.push_back(leftText->c_str()[i]);
        }
    }
    for (int32_t j = 0; j < (int32_t) rightText->length(); j++) {
        if (unionSet.find(rightText->c_str()[j]) == std::string::npos) {
            unionSet.push_back(rightText->c_str()[j]);
        }
        const char* pos = std::find(leftText->c_str(), leftText->c_str() + leftText->length(), rightText->c_str()[j]);
        if (intersectionSet.find(rightText->c_str()[j]) == std::string::npos && pos != leftText->c_str() + leftText->length()) {
            intersectionSet.push_back(rightText->c_str()[j]);
        }
    }
    double intersectionLength = intersectionSet.length();
    double unionLength = unionSet.length();
    return intersectionLength / unionLength;
}

Value<> JaccardDistance::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> leftValue = leftSubExpression->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value<> rightValue = rightSubExpression->execute(record);

    if (leftValue->isType<Text>() && rightValue->isType<Text>()) {
        return FunctionCall<>("textJaccard",
                              textJaccard,
                              leftValue.as<Text>()->getReference(),
                              rightValue.as<Text>()->getReference());
    } else {
        // If no type was applicable we throw an exception.
        NES_THROW_RUNTIME_ERROR("This expression is only defined on input arguments that are Text.");
    }
}

}// namespace NES::Runtime::Execution::Expressions