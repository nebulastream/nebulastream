#include <Execution/CustomDataTypes/Text.hpp>

namespace NES::Runtime::Execution::Expressions {

class RawText {
  public:
    const int32_t length;
    char* str;
};

bool textEquals(void* leftTextPtr, void* rightTextPtr) {

}

Value<Boolean> Text::equals(const Value<NES::Runtime::Execution::Expressions::Text>& other) const {}

}// namespace NES::Runtime::Execution::Expressions