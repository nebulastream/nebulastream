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
#include <Nautilus/Interface/DataTypes/List/ListValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <cstring>
#include <iostream>
#include <string>

namespace NES::Nautilus {

template<typename T>
ListValue<T>* ListValue<T>::create(uint32_t size) {
    auto* provider = Runtime::WorkerContext::getBufferProviderTLS();
    auto optBuffer = provider->getUnpooledBuffer(size * FIELD_SIZE + DATA_FIELD_OFFSET);
    if (!optBuffer.has_value()) {
        NES_THROW_RUNTIME_ERROR("Buffer allocation failed for text");
    }
    auto buffer = optBuffer.value();
    buffer.retain();
    return new (buffer.getBuffer()) ListValue<T>(size);
}

template<typename T>
ListValue<T>* ListValue<T>::create(const T* data, uint32_t size) {
    auto* list = create(size);
    std::memcpy(list->data(), data, size * FIELD_SIZE);
    return list;
}

template<typename T>
ListValue<T>::ListValue(uint32_t size) : size(size) {}

template<typename T>
T* ListValue<T>::data() {
    return reinterpret_cast<T*>(this + DATA_FIELD_OFFSET);
}

template<typename T>
const T* ListValue<T>::c_data() const {
    return reinterpret_cast<const T*>(this + DATA_FIELD_OFFSET);
}

template<typename T>
uint32_t ListValue<T>::length() const {
    return size;
}

template<typename T>
ListValue<T>* ListValue<T>::load(Runtime::TupleBuffer& buffer) {
    buffer.retain();
    return reinterpret_cast<ListValue*>(buffer.getBuffer());
}

template<class T>
bool ListValue<T>::equals(const ListValue<T>* other) const {
    if (length() != other->length()) {
        return false;
    }
    // mem compare of both underling arrays.
    return std::memcmp(c_data(), other->c_data(), size * sizeof(T)) == 0;
}

template<typename T>
ListValue<T>::~ListValue() {
    // A list value always is backed by the data region of a tuple buffer.
    // In the following, we recycle the tuple buffer and return it to the buffer pool.
    Runtime::recycleTupleBuffer(this);
}

// Instantiate ListValue types
template class ListValue<int8_t>;
template class ListValue<int16_t>;
template class ListValue<int32_t>;
template class ListValue<int64_t>;
template class ListValue<uint8_t>;
template class ListValue<uint16_t>;
template class ListValue<uint32_t>;
template class ListValue<uint64_t>;
template class ListValue<float>;
template class ListValue<double>;

}// namespace NES::Nautilus