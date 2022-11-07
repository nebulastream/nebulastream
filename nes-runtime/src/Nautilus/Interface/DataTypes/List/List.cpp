#include <Nautilus/Interface/DataTypes/List/List.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
namespace NES::Nautilus {

List::~List() {
    NES_DEBUG("~List");
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
Value<Int32> TypedList<BaseType>::length() {
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

template<IsListComponentType BaseType>
Value<> TypedList<BaseType>::read(Value<Int32>& index) const {
    return FunctionCall("readListIndex", readListIndex<ComponentType>, rawReference, index);
}

template<class T>
void writeListIndex(ListValue<T>* list, int32_t index, T value) {
    list->data()[index] = value;
}

template<IsListComponentType BaseType>
void TypedList<BaseType>::write(Value<Int32>& index, const Value<>& value) {
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