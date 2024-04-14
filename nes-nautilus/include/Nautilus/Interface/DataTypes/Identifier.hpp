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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_IDENTIFIER_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_IDENTIFIER_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>

namespace NES::Nautilus {
/**
 * @brief Abstract Nautilus NESIdentifier Adapter
 */
class Identifier : public TraceableType {
  public:
    Identifier(const TypeIdentifier* ti) : TraceableType(ti) {}

    virtual const std::shared_ptr<Boolean> equals(const Identifier&) const = 0;
    virtual const std::shared_ptr<Boolean> lessThan(const Identifier& other) const = 0;
    virtual const std::shared_ptr<Boolean> greaterThan(const Identifier& other) const = 0;
    virtual uint64_t getUnderlyingRawValue() const = 0;
    static bool isIdentifier(const Any& val);
};

template<std::integral T>
constexpr IR::Types::IntegerStamp::BitWidth getBitWidth() {
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                  "Nautilus only handles BitWidth up to 64 Bits");
    switch (sizeof(T)) {
        case 1: return IR::Types::IntegerStamp::BitWidth::I8;
        case 2: return IR::Types::IntegerStamp::BitWidth::I16;
        case 4: return IR::Types::IntegerStamp::BitWidth::I32;
        case 8: return IR::Types::IntegerStamp::BitWidth::I64;
    }
}

/**
 * @brief Concrete Nautilus NESIdentifier Adapter
 */
template<NESIdentifier T>
class IdentifierImpl final : public Identifier {
    using RawType = T::Underlying;

  public:
    static const inline auto type = TypeIdentifier::create<IdentifierImpl<T>>();
    IdentifierImpl(T identifier) : Identifier(&type), value(identifier.getRawValue()) {}
    uint64_t getUnderlyingRawValue() const override { return static_cast<uint64_t>(value); }
    std::shared_ptr<Any> copy() override { return create<IdentifierImpl<T>>(getValue()); }
    T getValue() { return T(value); }
    const std::shared_ptr<Boolean> equals(const Identifier& identifier) const override {
        return create<Boolean>(value == identifier.staticCast<IdentifierImpl>().value);
    }
    const std::shared_ptr<Boolean> lessThan(const Identifier& other) const override {
        return create<Boolean>(value < other.staticCast<IdentifierImpl>().value);
    }
    const std::shared_ptr<Boolean> greaterThan(const Identifier& other) const override {
        return create<Boolean>(value > other.staticCast<IdentifierImpl>().value);
    }

    IR::Types::StampPtr getType() const override {
        if constexpr (std::is_unsigned_v<typename T::Underlying>) {
            return std::make_shared<IR::Types::IntegerStamp>(getBitWidth<typename T::Underlying>(),
                                                             IR::Types::IntegerStamp::SignednessSemantics::Unsigned);
        } else {
            return std::make_shared<IR::Types::IntegerStamp>(getBitWidth<typename T::Underlying>(),
                                                             IR::Types::IntegerStamp::SignednessSemantics::Signed);
        }
    }

  private:
    // We only store the underlying value to not introduce additional types visible during debugging.
    typename T::Underlying value;
};
}// namespace NES::Nautilus

#endif//NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_IDENTIFIER_HPP_
