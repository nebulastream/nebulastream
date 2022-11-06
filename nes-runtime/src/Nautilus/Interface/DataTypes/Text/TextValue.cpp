#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
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

}// namespace NES::Nautilus