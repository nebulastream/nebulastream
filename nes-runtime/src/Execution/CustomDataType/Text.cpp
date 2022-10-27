#include <Execution/CustomDataTypes/Text.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
namespace NES::Runtime::Execution::Expressions {

class RawText {
  public:
    RawText(int32_t length) {
        auto* provider = Runtime::WorkerContext::getBufferProvider();
        auto optBuffer = provider->getUnpooledBuffer(length);
        NES_ASSERT2_FMT(!!optBuffer, "Cannot allocate buffer of size " << length);
        buffer = optBuffer.value();
    }
    RawText(const void* ptr) { buffer = *(Runtime::TupleBuffer*) (ptr); };
    RawText(const std::string& string) { auto stringSize = string.length(); };
    bool isUTF8() const { return buffer.getNumberOfTuples() == 1; };
    uint32_t length() const { return buffer.getBufferSize(); };
    const char* str() const { return buffer.getBuffer<char>(); };

  private:
    TupleBuffer buffer;
};

bool textEquals(void* leftTextPtr, void* rightTextPtr) {
    auto leftText = RawText(leftTextPtr);
    auto rightText = RawText(rightTextPtr);
    auto thirdText = RawText(25);
    char8_t x = "a";
    char8_t y = u8"a";
    char8_t* z = u8"ス";
}

Value<Boolean> Text::equals(const Value<NES::Runtime::Execution::Expressions::Text>& other) const {}

}// namespace NES::Runtime::Execution::Expressions