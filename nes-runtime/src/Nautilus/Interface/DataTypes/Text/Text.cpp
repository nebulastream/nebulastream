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
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Nautilus/Interface/DataTypes/List/List.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <cstring>
#include <iostream>
#include <memory>
#include <regex>
#include <string>

namespace NES::Nautilus {

Value<Text> transformReturnValues(TextValue* value) {
    auto textRef = TypedRef<TextValue>(value);
    return Value<Text>(std::make_unique<Text>(textRef));
}

Text::Text(TypedRef<NES::Nautilus::TextValue> rawReference) : Any(&type), rawReference(rawReference){};

bool textEquals(const TextValue* leftText, const TextValue* rightText) {
    NES_DEBUG("Check if {} and {} are equal", leftText->c_str(), rightText->c_str())
    if (leftText->length() != rightText->length()) {
        return false;
    } else if (std::memcmp(leftText->c_str(), rightText->c_str(), leftText->length()) == 0) {
        return true;
    }
    return false;
}

Value<Boolean> Text::equals(const Value<Text>& other) const {
    return FunctionCall<>("textEquals", textEquals, rawReference, other.value->rawReference);
}

TextValue* textSubstring(const TextValue* text, uint32_t index, uint32_t len) {
    if (text->length() < index + len) {
        NES_THROW_RUNTIME_ERROR("Text was not long enough");
    }
    auto resultText = TextValue::create(len);
    uint32_t j = 0;
    for (uint32_t i = index - 1; i < index + len - 1; i++) {
        resultText->str()[j] = text->c_str()[i];
        j++;
    }
    return resultText;
}
Value<Text> Text::substring(Value<UInt32> index, Value<UInt32> len) const {
    return FunctionCall<>("textSubstring", textSubstring, rawReference, index, len);
}

TextValue* textConcat(const TextValue* leftText, const TextValue* rightText) {
    uint32_t len = leftText->length() + rightText->length();
    auto resultText = TextValue::create(len);
    uint32_t i;
    for (i = 0; i < leftText->length(); i++) {
        resultText->str()[i] = leftText->c_str()[i];
    }
    for (uint32_t k = 0; k < rightText->length(); k++) {
        resultText->str()[k + i] = rightText->c_str()[k];
    }
    return resultText;
}
Value<Text> Text::concat(const Value<Text>& other) const {
    return FunctionCall<>("textConcat", textConcat, rawReference, other.value->rawReference);
}

bool textPrefix(const TextValue* leftText, const TextValue* rightText) {
    if (rightText->length() > leftText->length()) {
        NES_THROW_RUNTIME_ERROR("prefixText is longer than sourceText");
    }
    if (leftText->length() < rightText->length()) {
        return false;
    }
    return std::memcmp(leftText->c_str(), rightText->c_str(), rightText->length()) == 0;
}

Value<Boolean> Text::prefix(const Value<Text>& other) const {
    return FunctionCall<>("textPrefix", textPrefix, rawReference, other.value->rawReference);
}

TextValue* textRepeat(const TextValue* text, uint32_t n) {
    if (n <= 0) {
        NES_THROW_RUNTIME_ERROR("repetitions require a positive int, but received n<=0");
    }
    uint32_t len = text->length() * n;
    auto resultText = TextValue::create(len);
    uint32_t k = 0;
    for (uint32_t i = 1; i <= n; i++) {
        for (uint32_t j = 0; j < text->length(); j++) {
            resultText->str()[k] = text->c_str()[j];
            k++;
        }
    }
    return resultText;
}

Value<Text> Text::repeat(Value<UInt32> repeat_times) const {
    return FunctionCall<>("textRepeat", textRepeat, rawReference, repeat_times);
}

TextValue* textReverse(const TextValue* text) {
    auto resultText = TextValue::create(text->length());
    for (uint32_t i = 0; i < text->length(); i++) {
        uint32_t j = text->length() - 1 - i;
        resultText->str()[i] = text->c_str()[j];
    }
    return resultText;
}

Value<Text> Text::reverse() const { return FunctionCall<>("textReverse", textReverse, rawReference); }

uint32_t textPosition(const TextValue* text, const TextValue* target) {
    uint32_t pos = 0;
    bool match = false;
    if (text->length() < target->length()) {
        NES_THROW_RUNTIME_ERROR("TargetText length is longer than sourceText length");
    }
    for (uint32_t i = 0; i < text->length(); i++) {
        if (text->c_str()[i] == target->c_str()[0] && i + target->length() <= text->length()) {
            match = true;
            for (uint32_t j = 1; j < target->length() - 1; j++) {
                if (text->c_str()[j + i] != target->c_str()[j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                pos = i + 1;
                break;
            }
        }
    }
    return pos;
}

Value<UInt32> Text::position(Value<Text>& other) const {
    return FunctionCall<>("textPosition", textPosition, rawReference, other.value->rawReference);
}

TextValue* textReplace(const TextValue* text, const TextValue* source, const TextValue* target) {
    uint32_t p = textPosition(text, source);
    if (p == 0) {
        auto resultText = TextValue::create(text->length());
        std::memcpy(resultText->str(), text->c_str(), text->length());
        return resultText;
    }
    uint32_t startIndex = p - 1;
    uint32_t len = text->length() - source->length() + target->length();
    auto resultText = TextValue::create(len);

    uint32_t currentIndex = 0;
    for (uint32_t i = 0; i < startIndex; i++) {
        resultText->str()[i] = text->c_str()[i];
        currentIndex++;
    }
    uint32_t targetIndex = 0;
    for (uint32_t j = currentIndex; j < startIndex + target->length(); j++) {
        resultText->str()[j] = target->c_str()[targetIndex];
        targetIndex++;
        currentIndex++;
    }
    uint32_t restSourceIndex = startIndex + source->length();
    for (uint32_t k = currentIndex; k < len; k++) {
        resultText->str()[k] = text->c_str()[restSourceIndex];
        restSourceIndex++;
    }
    // if the text still contain the search sequence we have to replace again.
    if (textPosition(resultText, source) != 0) {
        auto tmpText = resultText;
        resultText = textReplace(tmpText, source, target);
        // The text value is manually allocated with create. So we have to free it manually.
        tmpText->~TextValue();
    }
    return resultText;
}

Value<Text> Text::replace(Value<Text>& source, Value<Text>& target) const {
    return FunctionCall<>("textReplace", textReplace, rawReference, source.value->rawReference, target.value->rawReference);
}

uint32_t TextGetLength(const TextValue* text) { return text->length(); }

const Value<UInt32> Text::length() const { return FunctionCall<>("textGetLength", TextGetLength, rawReference); }

AnyPtr Text::copy() { return std::make_shared<Text>(rawReference); }

int8_t readTextIndex(const TextValue* text, uint32_t index) { return text->c_str()[index]; }

void writeTextIndex(TextValue* text, uint32_t index, int8_t value) { text->str()[index] = value; }

Value<Int8> Text::read(Value<UInt32> index) { return FunctionCall<>("readTextIndex", readTextIndex, rawReference, index); }

void Text::write(Value<UInt32> index, Value<Int8> value) {
    FunctionCall<>("writeTextIndex", writeTextIndex, rawReference, index, value);
}

uint32_t getBitLength(const TextValue* text) { return (text->length() * 8_u32); }

const Value<UInt32> Text::bitLength() const { return FunctionCall<>("getBitLength", getBitLength, rawReference); }

TextValue* uppercaseText(const TextValue* text) {
    auto resultText = TextValue::create(text->length());
    for (uint32_t i = 0; i < text->length(); i++) {
        resultText->str()[i] = toupper(text->c_str()[i]);
    }
    return resultText;
}

const Value<Text> Text::upper() const { return FunctionCall<>("uppercaseText", uppercaseText, rawReference); }

TextValue* lowercaseText(const TextValue* text) {
    auto resultText = TextValue::create(text->length());
    for (uint32_t i = 0; i < text->length(); i++) {
        resultText->str()[i] = tolower(text->c_str()[i]);
    }
    return resultText;
}

const Value<Text> Text::lower() const { return FunctionCall<>("lowercaseText", lowercaseText, rawReference); }

TextValue* leftCharacters(const TextValue* text, uint32_t index) {
    if (index > text->length()) {
        index = text->length();
    }
    auto resultText = TextValue::create(index);
    for (uint32_t i = 0; i < index; i++) {
        resultText->str()[i] = text->c_str()[i];
    }
    return resultText;
}

const Value<Text> Text::left(Value<UInt32> index) {
    return FunctionCall<>("leftCharacters", leftCharacters, rawReference, index);
}

TextValue* rightCharacters(const TextValue* text, uint32_t index) {
    if (index > text->length()) {
        index = text->length();
    }
    auto resultText = TextValue::create(index);
    for (uint32_t i = 0; i < index; i++) {
        resultText->str()[i] = text->c_str()[i + index];
    }
    return resultText;
}

const Value<Text> Text::right(Value<UInt32> index) {
    return FunctionCall<>("rightCharacters", rightCharacters, rawReference, index);
}

TextValue* rightPad(const TextValue* text, uint32_t targetLength, int8_t value) {
    auto resultText = TextValue::create(targetLength);
    for (uint32_t i = 0; i < text->length(); i++) {
        resultText->str()[i] = text->c_str()[i];
    }
    for (uint32_t p = text->length(); p < targetLength; p++) {
        resultText->str()[p] = value;
    }
    return resultText;
}

const Value<Text> Text::rpad(Value<UInt32> index, Value<Int8> value) {
    return FunctionCall<>("rightPad", rightPad, rawReference, index, value);
}

TextValue* leftPad(const TextValue* text, uint32_t targetLength, int8_t value) {
    auto resultText = TextValue::create(targetLength);
    for (uint32_t p = 0; p < targetLength; p++) {
        resultText->str()[p] = value;
    }
    auto count = 0;
    for (uint32_t i = (targetLength - text->length()); i < targetLength; i++) {
        resultText->str()[i] = text->c_str()[count];
        count++;
    }

    return resultText;
}

const Value<Text> Text::lpad(Value<UInt32> index, Value<Int8> value) {
    return FunctionCall<>("leftPad", leftPad, rawReference, index, value);
}

TextValue* leftTrim(const TextValue* text, const TextValue* target) {
    if (text->length() <= 1) {
        NES_THROW_RUNTIME_ERROR("Text was not long enough");
    }
    auto position = uint32_t(textPosition(text, target));
    char space = ' ';
    auto spaceCount = 0_u32;
    for (uint32_t i = 0; i < position; i++) {
        char temp = text->c_str()[i];
        if (temp == space) {
            spaceCount++;
        } else {
            break;
        }
    }
    auto resultText = TextValue::create(text->length() - spaceCount);
    std::memcpy(resultText->str(), text->c_str() + spaceCount, text->length() - spaceCount);
    return resultText;
}

const Value<Text> Text::ltrim(Value<Text>& other) const {
    return FunctionCall<>("leftTrim", leftTrim, rawReference, other.value->rawReference);
}

TextValue* rightTrim(const TextValue* text, const TextValue* target) {
    if (text->length() <= 1) {
        NES_THROW_RUNTIME_ERROR("Text was not long enough");
    }
    auto position = uint32_t(textPosition(text, target));

    auto resultText = TextValue::create(target->length());
    std::memcpy(resultText->str(), text->c_str(), text->length() + position);

    return resultText;
}

const Value<Text> Text::rtrim(Value<Text>& other) const {
    return FunctionCall<>("rightTrim", rightTrim, rawReference, other.value->rawReference);
}

TextValue* lrTrim(const TextValue* text) {
    if (text->length() <= 1) {
        NES_THROW_RUNTIME_ERROR("Text was not long enough");
    }
    char space = ' ';
    auto spacecount = 0_u32;
    auto count = 0_u32;
    for (uint32_t i = 0; i < text->length(); i++) {
        char temp = text->c_str()[i];
        if (temp == space) {
            spacecount++;
        }
    }
    auto resultText = TextValue::create(text->length() - spacecount);

    for (uint32_t p = 0; p < text->length(); p++) {
        auto temp = text->c_str()[p];
        if (temp != space) {
            resultText->str()[count] = text->c_str()[p];
            count++;
        }
    }

    return resultText;
}

bool textSimilarTo(const TextValue* text, TextValue* pattern) {
    std::string target = std::string(text->c_str(), text->length());
    NES_DEBUG("Received the following source string {}", target);
    std::string strPattern = std::string(pattern->str(), pattern->length());
    NES_DEBUG("Received the following source string {}", strPattern);
    std::regex regexPattern(strPattern);
    return std::regex_match(target, regexPattern);
}

const Value<Boolean> Text::similarTo(Value<Text>& pattern) const {
    return FunctionCall<>("textSimilarTo", textSimilarTo, rawReference, pattern.value->rawReference);
}

bool textLike(const TextValue* text, TextValue* inputPattern, Boolean caseSensitive) {
    auto pattern = std::string(inputPattern->c_str(), inputPattern->length());
    NES_DEBUG("Checking in textLike if {} and {} are a match.", text->c_str(), pattern);

    Util::findAndReplaceAll(pattern, "_", ".");
    Util::findAndReplaceAll(pattern, "%", ".*?");
    pattern = "^" + pattern + "$";

    std::string target = std::string(text->c_str(), text->length());
    NES_DEBUG("Received the following source string {}", target);
    NES_DEBUG("Received the following source string {}", pattern);
    // LIKE and GLOB adoption requires syntax conversion functions
    // would make regex case in sensitive for LIKE
    if (caseSensitive.getValue()) {
        std::regex regexPattern(pattern, std::regex::icase);
        return std::regex_match(target, regexPattern);
    } else {
        std::regex regexPattern(pattern);
        return std::regex_match(target, regexPattern);
    }
}

const Value<Boolean> Text::like(Value<Text>& pattern, Value<Boolean> caseSensitive) const {
    return FunctionCall<>("textLike", textLike, rawReference, pattern.value->rawReference, caseSensitive);
}

const Value<Text> Text::trim() const { return FunctionCall<>("lrTrim", lrTrim, rawReference); }

IR::Types::StampPtr Text::getType() const { return rawReference.getType(); }
const TypedRef<TextValue>& Text::getReference() const { return rawReference; }

}// namespace NES::Nautilus