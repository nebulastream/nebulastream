#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_List_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_List_HPP_

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/List/ListValue.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
namespace NES::Nautilus {

class List : public Nautilus::Any {
  public:
    static const inline auto type = TypeIdentifier::create<List>();
    List(const TypeIdentifier* type) : Any(type){};
    virtual Value<Int32> length() = 0;
    virtual Value<Boolean> equals(const Value<List>& otherList) = 0;
    virtual ~List() override;
};

template<typename BaseType>
class TypedList : public List {
  public:
    using ComponentType = typename BaseType::RawType;
    using RawType = ListValue<ComponentType>;
    static const inline auto type = TypeIdentifier::create<TypedList<BaseType>>();
    TypedList(TypedRef<ListValue<int32_t>> ref) : List(&type), rawReference(ref) {}
    Value<Boolean> equals(const Value<List>& otherList) override {
        if (getTypeIdentifier() != otherList->getTypeIdentifier()) {
            return false;
        }
        if (this->length() != otherList->length()) {
            return false;
        }
        auto value = otherList.as<TypedList<BaseType>>();
        return FunctionCall("listEquals", listEquals<ComponentType>, rawReference, value->rawReference);
    }
    Value<Int32> length() override { return FunctionCall("getLength", getLength<ComponentType>, rawReference); }
    AnyPtr copy() override { return std::make_shared<TypedList<BaseType>>(rawReference); };

    const TypedRef<RawType> rawReference;
};
}// namespace NES::Nautilus
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_List_HPP_
