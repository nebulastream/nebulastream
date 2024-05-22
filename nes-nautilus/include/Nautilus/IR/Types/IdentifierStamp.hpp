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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_IDENTIFIERSTAMP_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_IDENTIFIERSTAMP_HPP_
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/IR/Types/Stamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <memory>

namespace NES::Nautilus::IR::Types {

class IdentifierStamp : public Stamp {
  public:
    static const inline auto type = TypeIdentifier::create<IdentifierStamp>();
    virtual const std::string toString() const = 0;
    virtual const StampPtr& getPhysicalStamp() const = 0;
    virtual std::string_view getTypeName() const = 0;
    explicit IdentifierStamp() : Stamp(&type) {}
};

template<class T>
constexpr std::string_view get_name() {
    char const* p = __PRETTY_FUNCTION__;
    while (*p++ != '=')
        ;
    for (; *p == ' '; ++p)
        ;
    char const* p2 = p;
    int count = 1;
    for (;; ++p2) {
        switch (*p2) {
            case '[': ++count; break;
            case ']':
                --count;
                if (!count)
                    return {p, std::size_t(p2 - p)};
        }
    }
    return {};
}

template<NESIdentifier IdentifierType>
class IdentifierStampImpl : public IdentifierStamp {
    constexpr static auto IdentifierTypeName = get_name<IdentifierType>();

  public:
    IdentifierStampImpl() {
        if constexpr (std::is_unsigned_v<typename IdentifierType::Underlying>) {
            physicalStamp = std::make_shared<IntegerStamp>(getBitWidth<typename IdentifierType::Underlying>(),
                                                           IntegerStamp::SignednessSemantics::Unsigned);
        } else {
            physicalStamp = std::make_shared<IntegerStamp>(getBitWidth<typename IdentifierType::Underlying>(),
                                                           IntegerStamp::SignednessSemantics::Signed);
        }
    }

    const std::string toString() const override { return fmt::format("IdentifierType: {}", typeid(IdentifierType).name()); };

    [[nodiscard]] const StampPtr& getPhysicalStamp() const { return physicalStamp; }

    std::string_view getTypeName() const override { return IdentifierTypeName; }

  private:
    template<std::integral T>
    constexpr IntegerStamp::BitWidth getBitWidth() {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                      "Nautilus only handles BitWidth up to 64 Bits");
        switch (sizeof(T)) {
            case 1: return IntegerStamp::BitWidth::I8;
            case 2: return IntegerStamp::BitWidth::I16;
            case 4: return IntegerStamp::BitWidth::I32;
            case 8: return IntegerStamp::BitWidth::I64;
            default: NES_NOT_IMPLEMENTED();
        }
    }
    StampPtr physicalStamp;
};

}// namespace NES::Nautilus::IR::Types

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_IR_TYPES_IDENTIFIERSTAMP_HPP_
