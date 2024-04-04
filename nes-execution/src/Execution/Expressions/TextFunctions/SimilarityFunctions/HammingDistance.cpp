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
#include <Execution/Expressions/TextFunctions/SimilarityFunctions/HammingDistance.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <string>

namespace NES::Runtime::Execution::Expressions {

HammingDistance::HammingDistance(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                                 const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), rightSubExpression(rightSubExpression) {}

/**
  * @brief Compares two text object and returns their Hamming Distance, -1 if inputs have different lengths.
  * @param leftText text object
  * @param rightText text object
  * @return Hamming Distance as Integer.
  */
uint64_t textHamming(const TextValue* leftText, const TextValue* rightText) {
    uint64_t distance = 0;
    /** throw exception for unequal length */
    if (leftText->length() != rightText->length()) {
        throw std::invalid_argument("Hamming Distance is only defined for inputs of equal length: the left string length is "
                                    + std::to_string(leftText->length()) + "and the right string length is  "
                                    + std::to_string(rightText->length()));
    }
    for (uint64_t i = 0; i < (uint64_t) leftText->length(); i++) {
        if (leftText->c_str()[i] != rightText->c_str()[i]) {
            distance++;
        }
    }
    return distance;
}

Value<> HammingDistance::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> leftValue = leftSubExpression->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value<> rightValue = rightSubExpression->execute(record);

    if (leftValue->isType<Text>() && rightValue->isType<Text>()) {
        return FunctionCall<>("textHamming",
                              textHamming,
                              leftValue.as<Text>()->getReference(),
                              rightValue.as<Text>()->getReference());
    } else {
        // If no type was applicable we throw an exception.
        NES_THROW_RUNTIME_ERROR("This expression is only defined on input arguments that are Text.");
    }
}

}// namespace NES::Runtime::Execution::Expressions