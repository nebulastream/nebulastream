#include <Nautilus/Interface/DataTypes/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/WorkerContext.hpp>
#include <cstring>
#include <iostream>
#include <string>

namespace NES::Nautilus {

RawText::RawText(int32_t length) {
    auto* provider = Runtime::WorkerContext::getBufferProviderTLS();
    auto optBuffer = provider->getUnpooledBuffer(length);
    NES_ASSERT2_FMT(!!optBuffer, "Cannot allocate buffer of size " << length);
    buffer = optBuffer.value();
    buffer.setNumberOfTuples(length);
}

RawText::RawText(const std::string& string) : RawText(string.length()) { std::memcpy(str(), string.c_str(), string.length()); }
uint32_t RawText::length() const { return buffer.getNumberOfTuples(); };

char* RawText::str() { return buffer.getBuffer<char>(); };

bool textEquals(const RawText& leftText, const RawText& rightText) {
    if (leftText.length() != rightText.length()) {
        return false;
    }
    return std::memcmp(leftText.c_str(), rightText.c_str(), leftText.length()) == 0;
}

Value<Boolean> Text::equals(const Value<Text>& other) const {
    return FunctionCall<>("textEquals", textEquals, rawReference, other.value->rawReference);
}

int32_t TextGetLength(const RawText& text) { return text.length(); }

const Value<Int32> Text::length() const { return FunctionCall<>("textEquals", TextGetLength, rawReference); }

Text::~Text() {
    // NES_DEBUG("~Text:" << std::to_string(*rawReference.value->value))
    //FunctionCall<>("destructTest", destructTest, rawReference.);
}
AnyPtr Text::copy() { return NES::Nautilus::AnyPtr(); }

int8_t readTextIndex(const RawText& text, int32_t index) { return text.c_str()[index]; }
void writeTextIndex(RawText&& text, int32_t index, int8_t value) { text.str()[index] = value; }

Value<Int8> Text::read(Value<Int32> index) { return FunctionCall<>("readTextIndex", readTextIndex, rawReference, index); }

void Text::write(Value<Int32> index, Value<Int8> value) {
    FunctionCall<>("writeTextIndex", writeTextIndex, rawReference, index, value);
}

}// namespace NES::Nautilus