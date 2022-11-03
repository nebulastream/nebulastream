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

class RawText {
  public:
    RawText(const void*&) {}
    RawText(int32_t length);
    RawText(const std::string& string);
    uint32_t length() const;
    char* str();
    const char* c_str() const { return buffer.getBuffer<char>(); };

  private:
    Runtime::TupleBuffer buffer;
};

class Text : public Nautilus::Any {
  public:
    static const inline auto type = TypeIdentifier::create<Text>();
    Text(TypedRef<RawText> rawReference) : Any(&type), rawReference(rawReference){};
    Value<Boolean> equals(const Value<Text>& other) const;
    AnyPtr copy() override;
    const Value<Int32> length() const;
    Value<Int8> read(Value<Int32> index);
    void write(Value<Int32> index, Value<Int8> value);
    ~Text() override;

  private:
    const TypedRef<RawText> rawReference;
};
}// namespace NES::Nautilus
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_
