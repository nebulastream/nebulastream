#include <Nautilus/Interface/DataTypes/List/ListValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <cstring>
#include <iostream>
#include <string>

namespace NES::Nautilus {

template<typename T>
int32_t ListValue<T>::length() const {
    return size;
}

template<typename T>
ListValue<T>* ListValue<T>::create(int32_t size) {
    auto* provider = Runtime::WorkerContext::getBufferProviderTLS();
    auto optBuffer = provider->getUnpooledBuffer(size);
    if (!optBuffer.has_value()) {
        NES_THROW_RUNTIME_ERROR("Buffer allocation failed for text");
    }
    auto buffer = optBuffer.value();
    buffer.retain();
    return new (buffer.getBuffer()) ListValue<T>(size);
}
template<typename T>
ListValue<T>::ListValue(int32_t size) : size(size) {}

template<typename T>
ListValue<T>* ListValue<T>::load(Runtime::TupleBuffer& buffer) {
    buffer.retain();
    return reinterpret_cast<ListValue*>(buffer.getBuffer());
}

template<typename T>
ListValue<T>::~ListValue() {
    // A list value always is backed by the data region of a tuple buffer.
    // In the following, we recycle the tuple buffer and return it to the buffer pool.
    Runtime::recycleTupleBuffer(this);
}

template class ListValue<int32_t>;

}// namespace NES::Nautilus