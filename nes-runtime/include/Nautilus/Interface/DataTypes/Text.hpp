#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
namespace NES::Nautilus {

class TextValue final {
  public:
    static constexpr size_t DATA_FIELD_OFFSET = sizeof(int32_t);
    static TextValue* create(int32_t size);
    static TextValue* create(const std::string& string);
    static TextValue* load(Runtime::TupleBuffer& tupleBuffer);
    int32_t length() const;
    char* str();
    const char* c_str() const;
    ~TextValue();
  private:
    TextValue(int32_t size);
    const int32_t size;
};

class Text : public Nautilus::Any {
  public:
    static const inline auto type = TypeIdentifier::create<Text>();
    Text(TypedRef<TextValue> rawReference) : Any(&type), rawReference(rawReference){};
    Value<Boolean> equals(const Value<Text>& other) const;
    AnyPtr copy() override;
    const Value<Int32> length() const;
    const Value<Text> upper() const;
    Value<Int8> read(Value<Int32> index);
    void write(Value<Int32> index, Value<Int8> value);
    IR::Types::StampPtr getType() const override;
    const TypedRef<TextValue> rawReference;
};
}// namespace NES::Nautilus
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_
