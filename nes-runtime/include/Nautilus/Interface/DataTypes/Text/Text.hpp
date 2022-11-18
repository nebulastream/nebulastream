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
#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Nautilus {

/**
 * @brief Nautilus value type for variable length text values.
 * The value type is physically represented by a TextValue.
 */
class Text final : public Nautilus::Any {
  public:
    static const inline auto type = TypeIdentifier::create<Text>();

    /**
     * @brief Constructor to create a text value object from a raw reference
     * @param rawReference
     */
    Text(TypedRef<TextValue> rawReference);

    /**
     * @brief Compares two text object and returns true if both text objects are equal.
     * @param other text object.
     * @return true if this and other text object are equal.
     */
    Value<Boolean> equals(const Value<Text>& other) const;

    /**
     * @brief Returns the number of characters of this text value.
     * @return Value<Int32>
     */
    const Value<UInt32> length() const;

    /**
     * @brief Converts this text to uppercase
     * @return Value<Text>
     */
    const Value<Text> upper() const;

    /**
     * @brief Reads one character from the text value at a specific index.
     * @param index as Value<Int32>
     * @return Value<Int8> as character
     */
    Value<Int8> read(Value<UInt32> index);

    /**
     * @brief Writes one character from the text value at a specific index.
     * @param index as Value<Int32>
     * @param value as Value<Int8>
     * @return Value<Int8> as character
     */
    void write(Value<UInt32> index, Value<Int8> value);

    /**
     * @brief Returns the stamp of this type
     * @return IR::Types::StampPtr
     */
    IR::Types::StampPtr getType() const override;

    /**
     * @brief Returns the underling reference
     * @return TypedRef<TextValue>&
     */
    [[maybe_unused]] const TypedRef<TextValue>& getReference() const;

    AnyPtr copy() override;

  private:
    const TypedRef<TextValue> rawReference;
};

template<typename T>
requires std::is_same_v<TextValue*, T>
auto createDefault() {
    auto textRef = TypedRef<TextValue>();
    auto text = Value<Text>(std::make_unique<Text>(textRef));
    return text;
}
}// namespace NES::Nautilus
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_CUSTOMDATATYPES_TEXT_HPP_
