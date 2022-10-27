#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_

#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Runtime::Execution::Expressions {

using namespace Nautilus;

class Text : public Any {
  public:
    static const inline auto type = TypeIdentifier::create<Text>();
    Text(Value<MemRef> rawReference) : Any(&type), rawReference(rawReference){};

    Value<Boolean> equals(const Value<Text>& other) const;

    AnyPtr copy() override;

  private:
    const Value<MemRef> rawReference;
};
}// namespace NES::Runtime::Execution::Expressions
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_
