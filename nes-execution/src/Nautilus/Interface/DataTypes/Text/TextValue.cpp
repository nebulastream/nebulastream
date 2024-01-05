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
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <iostream>
#include <string>

namespace NES::Nautilus {

TextValue::TextValue(uint32_t size) : size(size) {}

uint32_t TextValue::length() const { return size; }

char* TextValue::str() { return reinterpret_cast<char*>(this) + DATA_FIELD_OFFSET; }

const char* TextValue::c_str() const { return reinterpret_cast<const char*>(this) + DATA_FIELD_OFFSET; }

TextValue* TextValue::create(Runtime::TupleBuffer& buffer, uint32_t size) {
    buffer.retain();
    return new (buffer.getBuffer()) TextValue(size);
}

TextValue* TextValue::create(uint32_t size) {
    auto buffer = allocateBuffer(size);
    return create(buffer, size);
}

TextValue* TextValue::create(const std::string& string) {
    auto* textValue = create(string.length());
    std::memcpy(textValue->str(), string.c_str(), string.length());
    return textValue;
}

TextValue* TextValue::create(Runtime::TupleBuffer& buffer, const std::string& string) {
    auto* textValue = create(buffer, string.length());
    std::memcpy(textValue->str(), string.c_str(), string.length());
    return textValue;
}

TextValue* TextValue::load(Runtime::TupleBuffer& buffer) {
    buffer.retain();
    return reinterpret_cast<TextValue*>(buffer.getBuffer());
}

std::string TextValue::strn_copy() const {
    // We have to ensure that the string is null terminated.
    // To this end, we use the strndup function.
    char* nullTerminatedString = strndup(c_str(), length());
    auto resultString = std::string(nullTerminatedString);
    free(nullTerminatedString);
    return resultString;
}

Runtime::TupleBuffer TextValue::getBuffer() const { return Runtime::TupleBuffer::reinterpretAsTupleBuffer((void*) this); }

TextValue::~TextValue() {
    // A text value always is backed by the data region of a tuple buffer.
    // In the following, we recycle the tuple buffer and return it to the buffer pool.
    Runtime::recycleTupleBuffer(this);
}
Runtime::TupleBuffer TextValue::allocateBuffer(uint32_t size) {
    auto* provider = Runtime::WorkerContext::getBufferProviderTLS();
    auto optBuffer = provider->getUnpooledBuffer(size + DATA_FIELD_OFFSET);
    if (!optBuffer.has_value()) {
        NES_THROW_RUNTIME_ERROR("Buffer allocation failed for text");
    }
    return optBuffer.value();
}

}// namespace NES::Nautilus