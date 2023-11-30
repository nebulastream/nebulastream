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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_LIST_LISTVALUE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_LIST_LISTVALUE_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <string>
namespace NES::Nautilus {

class BaseListValue {};

/**
 * @brief Physical data type that represents a ListValue.
 * A list value is backed by an tuple buffer.
 * Physical layout:
 * | ----- size 4 byte ----- | ----- variable length data
 */
template<class T>
class ListValue final : public BaseListValue {
  public:
    using RawType = T;
    static constexpr size_t DATA_FIELD_OFFSET = sizeof(uint32_t);
    static constexpr size_t FIELD_SIZE = sizeof(T);

    /**
     * @brief Creates a ListValue for a specific number of elements.
     * @param size number of elements
     * @return ListValue*
     */
    static ListValue* create(uint32_t size);

    /**
     * @brief Creates a ListValue from a T* and a specific size.
     * @param size number of elements
     * @return ListValue*
     */
    static ListValue* create(const T* data, uint32_t size);

    /**
     * @brief Loads a ListValue from a TupleBuffer.
     * @return ListValue*
     */
    static ListValue* load(Runtime::TupleBuffer& tupleBuffer);

    /**
     * @brief Returns the length in the number of characters of the text value
     * @return int32_t
     */
    uint32_t length() const;

    /**
     * @brief compares this and another list.
     * @return boolean
     */
    bool equals(const ListValue<T>* other) const;

    /**
     * @brief Concatenates two lists.
     * @param other
     * @return
     */
    ListValue<T>* concat(const ListValue<T>* other) const;

    /**
     * @brief Appends element to list.
     * @param element
     * @return
     */
    ListValue<T>* append(T element) const;

    /**
     * @brief Prepends element to list.
     * @param element
     * @return
     */
    ListValue<T>* prepend(T element) const;

    /**
     * @brief Returns true if the list contains the element.
     * @param element
     * @return
     */
    bool contains(T element) const;

    /**
     * @brief Returns the index of the element if the list contains the element and -1 if element not found.
     * @param element
     * @return
     */
    int32_t listPosition(T element) const;

    /**
     * @brief Sorts the elements of the list.
     * @return
     */
    ListValue<T>* sort() const;

    /**
     * @brief Reverse the elements of the list.
     * @return
     */
    ListValue<T>* revers() const;

    /**
     * @brief Returns pointer to the underling array of values.
     * @return T*
     */
    T* data();

    /**
     * @brief Returns const pointer to the underling array of values.
     * @return T*
     */
    const T* c_data() const;

    /**
     * @brief Destructor for the text value that also releases the underling tuple buffer.
     */
    ~ListValue();

  private:
    /**
     * @brief Private constructor to initialize a new text
     * @param size
     */
    ListValue(uint32_t size);
    const uint32_t size;
};

template<class T>
uint32_t getLength(const ListValue<T>* list) {
    return list->length();
}

template<class T>
bool listEquals(const ListValue<T>* left, const ListValue<T>* right) {
    return left->equals(right);
}

}// namespace NES::Nautilus
#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_LIST_LISTVALUE_HPP_
