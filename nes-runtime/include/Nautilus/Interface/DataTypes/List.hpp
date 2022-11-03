#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_List_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_List_HPP_

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
namespace NES::Nautilus {

class RawList {
  public:
    RawList(int32_t length);
    uint32_t length() const;
    int8_t* data();
  private:
    Runtime::TupleBuffer buffer;
};


class List : public Nautilus::Any {
  public:
    static const inline auto type = TypeIdentifier::create<List>();
    List(const TypeIdentifier* type, TypedRef<RawList>& ref) : Any(type), rawReference(ref){};
    Value<Int32> length();
    ~List() override;

    const TypedRef<RawList> rawReference;
};

template<typename BaseType>
class TypedList : public List {
  public:
    static const inline auto type = TypeIdentifier::create<TypedList<BaseType>>();
    TypedList(TypedRef<RawList> ref) : List(&type, ref) {}
    AnyPtr copy() override { return std::make_shared<TypedList<BaseType>>(rawReference); };
};
}// namespace NES::Nautilus
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_List_HPP_
