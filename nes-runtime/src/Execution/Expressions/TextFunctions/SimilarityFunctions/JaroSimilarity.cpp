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
#include <Execution/Expressions/TextFunctions/SimilarityFunctions/JaroSimilarity.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <regex>
#include <string>

namespace NES::Runtime::Execution::Expressions {

JaroSimilarity::JaroSimilarity(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                               const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression,
                               const NES::Runtime::Execution::Expressions::ExpressionPtr& flagExpression)
    : leftSubExpression(leftSubExpression), rightSubExpression(rightSubExpression), flagExpression(flagExpression) {}

/**
 * Compares if two text objects are equivalent
 * @param leftText, rightText the text objects to compare
 * @return Integer, 0 = equivalent, else not
 */
int64_t textEquals(const TextValue* leftText, const TextValue* rightText) {
    if (leftText->length() != rightText->length()) {
        return -1;
    }
    return std::memcmp(leftText->c_str(), rightText->c_str(), leftText->length());
}

/**
  * @brief Compares two text object and returns their Jaro Similarity as Double.
  * @param leftText text object
  * @param rightText text object
  * @return Jaccard Distance as Integer.
  */
double textJaro(const TextValue* leftText, const TextValue* rightText) {
    /** if texts are equal we can save time */
    if (textEquals(leftText, rightText) == 0) {
        return 1.0;
    }

    /** maximum distance */
    int32_t maxSim = floor(fmax(leftText->length(), rightText->length()) / 2) - 1;
    int32_t match = 0;

    /** hashes for matches */
    auto leftHash = new int32_t[leftText->length()];
    auto rightHash = new int32_t[rightText->length()];
    for (int32_t i = 0; i < (int32_t) leftText->length(); i++) {
        leftHash[i] = 0;
    }
    for (int32_t i = 0; i < (int32_t) rightText->length(); i++) {
        rightHash[i] = 0;
    }

    /** first string */
    for (int32_t i = 0; i < (int32_t) leftText->length(); i++) {

        /** Check if there is any matches */
        for (int32_t j = fmax(0, i - maxSim); j < fmin(rightText->length(), i + maxSim + 1); j++) {

            /** If there is a match */
            if (leftText->c_str()[i] == rightText->c_str()[j] && rightHash[j] == 0) {
                leftHash[i] = 1;
                rightHash[j] = 1;
                match++;
                break;
            }
        }
    }

    /** no matches */
    if (match == 0) {
        return 0.0;
    }

    /** transposition counter */
    double transpositions = 0;
    int32_t point = 0;

    /** We search for matches with a third matched character between the indices */
    for (int32_t i = 0; i < (int32_t) leftText->length(); i++)
        if (leftHash[i]) {

            /** Find the next matched character
               in second string */
            while (rightHash[point] == 0)
                point++;

            if (leftText->c_str()[i] != rightText->c_str()[point++])
                transpositions++;
        }

    transpositions /= 2;

    return (((double) match) / ((double) leftText->length()) + ((double) match) / ((double) rightText->length())
            + ((double) match - transpositions) / ((double) match))
        / 3.0;
}

/**
  * @brief Compares two text object and returns their Jaro Winkler Distance (takes prefix into account) as Double.
  * @param leftText text object
  * @param rightText text object
  * @return Jaccard Distance as Integer.
  */
double textJaroWinkler(const TextValue* leftText, const TextValue* rightText, double jaroDist) {
    int32_t prefix = 0;

    /** check for prefix */
    for (int32_t i = 0; i < fmin(rightText->length(), leftText->length()); i++) {
        /** If the characters match */
        if (rightText->c_str()[i] == leftText->c_str()[i]) {
            prefix++;
        } else {
            break;
        }
    }
    /**
     * The Jaro-Winkler similarity uses a prefix scale p, which gives more favorable ratings to strings that match from
     * the beginning for a set prefix length l. p is a constant scaling factor for how much the score is adjusted
     * upwards for having common prefixes. The standard value for this constant in Winkler's work is p=0.1.
     * l is the length of common prefix at the start of the string (up to a maximum of 4 characters).
     * source: https://rosettacode.org/wiki/Jaro-Winkler_distance
     */
    prefix = fmin(4, prefix);

    /** calculate result */
    return jaroDist + 0.1 * prefix * (1 - jaroDist);
}

Value<> JaroSimilarity::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> leftValue = leftSubExpression->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value<> rightValue = rightSubExpression->execute(record);

    Value<> flagValue = flagExpression->execute(record);

    if (leftValue->isType<Text>() && rightValue->isType<Text>() && flagValue->isType<Boolean>()) {
        Value<> distance =
            FunctionCall<>("textJaro", textJaro, leftValue.as<Text>()->getReference(), rightValue.as<Text>()->getReference());
        if (flagValue.as<Boolean>()) {
            return FunctionCall<>("textJaroWinkler",
                                  textJaroWinkler,
                                  leftValue.as<Text>()->getReference(),
                                  rightValue.as<Text>()->getReference(),
                                  distance.as<Double>());
        }
        return distance;
    } else {
        // If no type was applicable we throw an exception.
        NES_THROW_RUNTIME_ERROR("This expression is only defined on input arguments that are Text and a Boolean for the Winkler "
                                "addition (favourable ratings og matching prefixes).");
    }
}

}// namespace NES::Runtime::Execution::Expressions