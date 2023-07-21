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
#include <Nautilus/Interface/DataTypes/List/List.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/StdInt.hpp>

namespace NES::Nautilus {

template<typename T>
auto transformReturnValues(ListValue<T>* value) {
    auto textRef = TypedRef<ListValue<T>>(value);
    using ComponentType = typename RawTypeToNautilusType<T>::type;
    return Value<TypedList<ComponentType>>(TypedList<ComponentType>(textRef));
}

template<typename T>
    requires std::is_base_of<BaseListValue, typename std::remove_pointer<T>::type>::value
auto createDefault() {
    using RawType = typename std::remove_pointer<T>::type::RawType;
    auto textRef = TypedRef<ListValue<RawType>>(nullptr);
    using ComponentType = typename RawTypeToNautilusType<RawType>::type;
    return Value<TypedList<ComponentType>>(TypedList<ComponentType>(textRef));
}

List::~List() { NES_DEBUG("~List"); }

List::ListValueIterator List::begin() {
    auto startIndex = Value<UInt32>((std::uint32_t) 0);
    return {*this, startIndex};
}

List::ListValueIterator List::end() {
    auto endIndex = length();
    return {*this, endIndex};
}

List::ListValueIterator::ListValueIterator(List& listRef, Value<UInt32>& currentIndex)
    : list(listRef), currentIndex(currentIndex) {}

Value<> List::ListValueIterator::operator*() { return list.read(currentIndex); }

bool List::ListValueIterator::operator==(const ListValueIterator& other) const { return currentIndex == other.currentIndex; }

List::ListValueIterator& List::ListValueIterator::operator++() {
    currentIndex = currentIndex + 1_u32;
    return *this;
}

template<IsListComponentType BaseType>
TypedList<BaseType>::TypedList(TypedRef<RawType> ref) : List(&type), rawReference(ref) {}

template<IsListComponentType BaseType>
Value<Boolean> TypedList<BaseType>::equals(const Value<List>& otherList) {
    if (getTypeIdentifier() != otherList->getTypeIdentifier()) {
        return false;
    }
    if (this->length() != otherList->length()) {
        return false;
    }
    auto value = otherList.as<TypedList<BaseType>>();
    return FunctionCall("listEquals", listEquals<ComponentType>, rawReference, value->rawReference);
}

template<IsListComponentType BaseType>
Value<UInt32> TypedList<BaseType>::length() {
    return FunctionCall("getLength", getLength<ComponentType>, rawReference);
}

template<IsListComponentType BaseType>
AnyPtr TypedList<BaseType>::copy() {
    return std::make_shared<TypedList<BaseType>>(rawReference);
}

template<class T>
T readListIndex(const ListValue<T>* list, int32_t index) {
    return list->c_data()[index];
}

template<class T>
ListValue<T>* reverseList(const ListValue<T>* i) {
    return ListValue<T>::create(i->length());
}

template<IsListComponentType BaseType>
Value<> TypedList<BaseType>::read(Value<UInt32>& index) const {
    return FunctionCall("readListIndex", readListIndex<ComponentType>, rawReference, index);
}

template<class T>
void writeListIndex(ListValue<T>* list, int32_t index, T value) {
    list->data()[index] = value;
}

template<IsListComponentType BaseType>
void TypedList<BaseType>::write(Value<UInt32>& index, const Value<>& value) {
    auto baseValue = value.as<BaseType>();
    FunctionCall("writeListIndex", writeListIndex<ComponentType>, rawReference, index, baseValue);
}

// Instantiate List types
template class TypedList<Int8>;
template class TypedList<Int16>;
template class TypedList<Int32>;
template class TypedList<Int64>;
template class TypedList<UInt8>;
template class TypedList<UInt16>;
template class TypedList<UInt32>;
template class TypedList<UInt64>;
template class TypedList<Float>;
template class TypedList<Double>;

}// namespace NES::Nautilus