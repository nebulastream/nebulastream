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

template<class T>
ListValue<T>* ListValue<T>::concat(const ListValue<T>* other) const {
    auto leftListSize = length();
    auto rightListSize = other->length();
    // create the result list, which is the current list length + the other list length
    auto resultList = ListValue<T>::create(leftListSize + rightListSize);

    // copy this list into result
    auto resultDataPtr = resultList->data();
    for (uint32_t i = 0; i < length(); i++) {
        resultDataPtr[i] = c_data()[i];
    }
    // copy other list in result
    for (uint32_t i = 0; i < other->length(); i++) {
        resultDataPtr[leftListSize + i] = c_data()[i];
    }
    return resultList;
}

template<class T>
ListValue<T>* ListValue<T>::append(T element) const {
    // create result list
    auto resultList = ListValue<T>::create(length() + 1);
    // TODO copy list and append on element to the result list
    auto resultDataPtr = resultList->data();
    for(uint32_t i = 0; i < length(); i++){
        if(length()==1){
            resultDataPtr[i] = element;
        }
        if(length()>1){
            resultDataPtr[length()-1] = element;
        }
    }
    return resultList;
}

template<class T>
ListValue<T>* ListValue<T>::prepend(T element) const {
    // create result list
    auto resultList = ListValue<T>::create(length() + 1);
    // TODO copy list and prepend on element to the result list
    return resultList;
}

template<class T>
bool ListValue<T>::contains(T element) const {

    // TODO check if the element is contained in the list and return true in this case

    for (uint32_t i = 0; i < length(); i++)   {
        if(c_data()[i] == element){
            return true;
        }
    }


    return false;
}

template<class T>
uint32_t ListValue<T>::listPosition(T element) const {
    // TODO return the index of the element. If it is not contained return 0.
    return 0;
}
template<class T>
ListValue<T>* ListValue<T>::sort() const {
    // create copy of list
    auto resultList = ListValue<T>::create(length());
    std::memcpy(resultList->data(), c_data(), length());
    auto resultDataPtr = resultList->data();
    // TODO sort content of list (resultList->data()): look at the following links for inspiration
    // https://stackoverflow.com/questions/56115960/can-i-use-stdsort-on-heap-allocated-raw-arrays
    // https://en.cppreference.com/w/cpp/algorithm/sort
    for (uint32_t i = 0; i < length(); i++) {
        for (uint32_t j = i+1; i < length(); j++) {
            uint32_t a = 0;
            if(c_data()[i]>c_data()[j]){
                a=c_data()[i];
                resultDataPtr[i] = c_data()[j];
                resultDataPtr[j] = a;
            }
        }
    }
    return resultList;
}
template<class T>
ListValue<T>* ListValue<T>::revers() const {
    // create copy of list
    auto resultList = ListValue<T>::create(length());
    std::memcpy(resultList->data(), c_data(), length());
    // TODO reverse content of list (resultList->data()): look at the following links for inspiration
    // https://cplusplus.com/reference/algorithm/reverse/
    return resultList;
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