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
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cstring>
#include <iostream>
#include <string>

namespace NES::Nautilus {

Value<Text> transformReturnValues(TextValue* value) {
    auto textRef = TypedRef<TextValue>(value);
    return Value<Text>(std::make_unique<Text>(textRef));
}

Text::Text(TypedRef<NES::Nautilus::TextValue> rawReference) : Any(&type), rawReference(rawReference){};

int64_t textEquals(const TextValue* leftText, const TextValue* rightText) {
    if (leftText->length() != rightText->length()) {
        return false;
    }
    return std::memcmp(leftText->c_str(), rightText->c_str(), leftText->length());
}

Value<Boolean> Text::equals(const Value<Text>& other) const {
    Value<Int64> result = FunctionCall<>("textEquals", textEquals, rawReference, other.value->rawReference);
    auto boolResult = result == Value<Int64>((int64_t) 0);
    return boolResult.as<Boolean>();
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

TextValue* uppercaseText(const TextValue* text) {
    auto resultText = TextValue::create(text->length());
    for (uint32_t i = 0; i < text->length(); i++) {
        resultText->str()[i] = toupper(text->c_str()[i]);
    }
    return resultText;
}

const Value<Text> Text::upper() const { return FunctionCall<>("uppercaseText", uppercaseText, rawReference); }

IR::Types::StampPtr Text::getType() const { return rawReference.getType(); }
const TypedRef<TextValue>& Text::getReference() const { return rawReference; }

}// namespace NES::Nautilus