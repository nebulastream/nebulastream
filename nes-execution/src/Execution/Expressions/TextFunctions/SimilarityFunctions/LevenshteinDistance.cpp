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
#include <Execution/Expressions/TextFunctions/SimilarityFunctions/LevenshteinDistance.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <string>

namespace NES::Runtime::Execution::Expressions {

LevenshteinDistance::LevenshteinDistance(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                                         const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), rightSubExpression(rightSubExpression) {}

/**
  * @brief Compares two text object and returns their LevenshteinDistance Distance as calculated with Wagner-Fischer Algorithm.
  * @param leftText text object
  * @param rightText text object
  * @return Hamming Distance as Integer.
  */
uint64_t textLevenshtein(const TextValue* leftText, const TextValue* rightText) {
    const uint64_t lengthLeft = leftText->length();
    /** This is a variant of the Wagner-Fischer algorithm which computes a matrix with rows and columns corresponding to letters in each string
      An entry matrix[i][j] contains the distance from first i characters of the left string to first j characters of the right string */
    NES_DEBUG("The left string length is {} and the right string length is {}",
              std::to_string(leftText->length()),
              std::to_string(rightText->length()));
    auto matrix = new uint64_t[lengthLeft + 1][2];
    /* first column corresponds to i insertions from empty string */
    for (uint64_t i = 0; i <= lengthLeft; i++) {
        matrix[i][0] = i;
        matrix[i][1] = 0;
    }
    matrix[0][1] = 1;

    int32_t subCost;
    for (uint64_t j = 1; j <= rightText->length(); j++) {
        for (uint64_t i = 1; i <= lengthLeft; i++) {
            /*+ subCost signifies whether substitution amounts to doing nothing (no increased distance) */
            if (leftText->c_str()[i - 1] == rightText->c_str()[j - 1]) {
                subCost = 0;
            } else {
                subCost = 1;
            }

            /* distance is updated based on deletion, insertion or substitution as "cheapest" next step */
            matrix[i][1] = fmin(fmin(matrix[i - 1][1] + 1,   /*  deletion */
                                     matrix[i][0] + 1),      /* insertion */
                                matrix[i - 1][0] + subCost); /* substitution */
        }
        /* "move" one row to the right */
        for (uint64_t i = 0; i <= lengthLeft; i++) {
            matrix[i][0] = matrix[i][1];
            matrix[i][1] = 0;
        }
        /* first row corresponds to j insertions from empty string */
        matrix[0][1] = j + 1;
    }
    return matrix[lengthLeft][0];
}

Value<> LevenshteinDistance::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> leftValue = leftSubExpression->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value<> rightValue = rightSubExpression->execute(record);

    if (leftValue->isType<Text>() && rightValue->isType<Text>()) {
        return FunctionCall<>("textLevenshtein",
                              textLevenshtein,
                              leftValue.as<Text>()->getReference(),
                              rightValue.as<Text>()->getReference());
    } else {
        // If no type was applicable we throw an exception.
        NES_THROW_RUNTIME_ERROR("This expression is only defined on input arguments that are Text.");
    }
}

}// namespace NES::Runtime::Execution::Expressions