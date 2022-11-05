#include <Nautilus/Interface/DataTypes/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/WorkerContext.hpp>
#include <cstring>
#include <iostream>
#include <string>

namespace NES::Nautilus {

int32_t TextValue::length() const { return size; }

char* TextValue::str() { return reinterpret_cast<char*>(this + DATA_FIELD_OFFSET); }
const char* TextValue::c_str() const { return reinterpret_cast<const char*>(this + DATA_FIELD_OFFSET); }

TextValue* TextValue::create(int32_t size) {
    auto* provider = Runtime::WorkerContext::getBufferProviderTLS();
    auto optBuffer = provider->getUnpooledBuffer(size);
    if (!optBuffer.has_value()) {
        NES_THROW_RUNTIME_ERROR("Buffer allocation failed for text");
    }
    auto buffer = optBuffer.value();
    buffer.retain();
    return new (buffer.getBuffer()) TextValue(size);
}

TextValue::TextValue(int32_t size) : size(size) {}

TextValue* TextValue::create(const std::string& string) {
    auto* textValue = create(string.length());
    std::memcpy(textValue->str(), string.c_str(), string.length());
    return textValue;
}

TextValue* TextValue::load(Runtime::TupleBuffer& buffer) {
    buffer.retain();
    return reinterpret_cast<TextValue*>(buffer.getBuffer());
}

TextValue::~TextValue() {
    // A text value always is backed by the data region of a tuple buffer.
    // In the following, we recycle the tuple buffer and return it to the buffer pool.
    Runtime::recycleTupleBuffer(this);
}

bool textEquals(const TextValue* leftText, const TextValue* rightText) {
    if (leftText->length() != rightText->length()) {
        return false;
    }
    return std::memcmp(leftText->c_str(), rightText->c_str(), leftText->length()) == 0;
}

Value<Boolean> Text::equals(const Value<Text>& other) const {
    return FunctionCall<>("textEquals", textEquals, rawReference, other.value->rawReference);
}

int32_t TextGetLength(const TextValue* text) { return text->length(); }

const Value<Int32> Text::length() const { return FunctionCall<>("textGetLength", TextGetLength, rawReference); }

AnyPtr Text::copy() { return NES::Nautilus::AnyPtr(); }

int8_t readTextIndex(const TextValue* text, int32_t index) { return text->c_str()[index]; }
void writeTextIndex(TextValue* text, int32_t index, int8_t value) { text->str()[index] = value; }

Value<Int8> Text::read(Value<Int32> index) { return FunctionCall<>("readTextIndex", readTextIndex, rawReference, index); }

void Text::write(Value<Int32> index, Value<Int8> value) {
    FunctionCall<>("writeTextIndex", writeTextIndex, rawReference, index, value);
}

TextValue* uppercaseText(const TextValue* text) {
    auto resultText = TextValue::create(text->length());
    for (int32_t i = 0; i < text->length(); i++) {
        resultText->str()[i] = toupper(text->c_str()[i]);
    }
    return resultText;
}

template<>
auto transformReturnValues(TextValue* value) {
    auto textRef = TypedRef<TextValue>(value);
    return Value<Text>(std::make_unique<Text>(textRef));
}

template<>
auto createDefault<TextValue*>() {
    auto textRef = TypedRef<TextValue>();
    auto text = Value<Text>(std::make_unique<Text>(textRef));
    return text;
}

const Value<Text> Text::upper() const {
    return FunctionCall<>("uppercaseText", uppercaseText, rawReference); }
IR::Types::StampPtr Text::getType() const { return rawReference.getType(); }

}// namespace NES::Nautilus