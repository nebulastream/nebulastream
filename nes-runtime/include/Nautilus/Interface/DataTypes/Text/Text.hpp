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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TEXT_TEXT_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TEXT_TEXT_HPP_

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/List/List.hpp>
#include <Nautilus/Interface/DataTypes/List/ListValue.hpp>
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
     * @brief  checks if the first Text starts with another.
     * @param other text object.
     * @return true if text objects starts with another.
     */
    Value<Boolean> prefix(const Value<Text>& other) const;

    /**
     * @brief repeats the TextObject repeatNumber times.
     * @param Value repeat_times.
     * @return rawReference.
     */
    Value<Text> repeat(Value<UInt32> repeat_times) const;

    /**
     * @brief Reverses the Text.
     * @return rawReference.
     */
    Value<Text> reverse() const;

    /**
     * @brief find the location of first occurrence of search_text in Text.
     * @param other text object(search_text).
     * @return location of first occurrence of search_text in Text, return 0 if no match found.
     */
    Value<UInt32> position(Value<Text>& other) const;

    /**
     * @brief Replace all occurrences of source in text with target
     * @param sourcetext, targettext.
     * @return Text Object
     */
    Value<Text> replace(Value<Text>& source, Value<Text>& target) const;

    /**
     * @brief concat two text object
     * @param other text object
     * @return combination of two text object
     */
    Value<Text> concat(const Value<Text>& other) const;

    /**
     * @brief  Extract a subText of length len from the startIndex.Note that the start value of 1 refers to the first character of the Text
     * @param  Value startIndex, Value len
     * @return a subtext object
     */
    Value<Text> substring(Value<UInt32>, Value<UInt32>) const;

    /**
     * @brief Returns the number of characters of this text value.
     * @return Value<Int32>
     */
    const Value<UInt32> length() const;

    /**
     * @brief Returns the number of bits of this text value.
     * @return Value<Int32>
     */
    const Value<UInt32> bitLength() const;

    /**
     * @brief Converts this text to uppercase.
     * @return Value<Text>
     */
    const Value<Text> upper() const;

    /**
     * @brief Converts this text to lowercase.
     * @return Value<Text>
     */
    const Value<Text> lower() const;

    /**
     * @brief Reads index characters from the left side of the text value.
     * @param index as Value<Int32>
     * @return Value<Text>
     */
    const Value<Text> left(Value<UInt32> index);

    /**
     * @brief Reads index characters from the right side of the text value.
     * @param index as Value<Int32>
     * @return Value<Text>
     */
    const Value<Text> right(Value<UInt32> index);

    /**
     * @brief Pads the string with the character from the right until it has count characters.
     * @param index as Value<Int32>, value as Value<Int8>
     * @return Value<Text>
     */
    const Value<Text> rpad(Value<UInt32> index, Value<Int8> value);

    /**
     * @brief Pads the string with the character from the left until it has count characters.
     * @param index as Value<Int32>, value as Value<Int8>
     * @return Value<Text>
     */
    const Value<Text> lpad(Value<UInt32> index, Value<Int8> value);

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
     * @brief Removes any spaces from the left side of the string
     * @param
     * @return Value<Text>
     */
    const Value<Text> ltrim(Value<Text>& other) const;

    /**
     * @brief Removes any spaces from the left side of the string
     * @param
     * @return Value<Text>
     */
    const Value<Text> rtrim(Value<Text>& other) const;

    /**
     * @brief Removes any spaces from the left and right side of the string
     * @param
     * @return Value<Text>
     */
    const Value<Text> trim() const;

    /**
     * @brief Returns true or false depending on whether its pattern matches the given string
     * @param compareText as Value<Text>
     * @return Value<Boolean>
     */
    const Value<Boolean> similarTo(Value<Text>& compareText) const;

    /**
     * @brief Returns true or false whether the string matches the supplied pattern
     * @param compareText as Value<Text>
     * @param caseSensitive as Value<Boolean> true for case sensitive and false for insensitive pattern matching
     * @return Value<Boolean> returns true if the string matches the supplied pattern
     */
    const Value<Boolean> like(Value<Text>& compareText, Value<Boolean> caseSensitive) const;

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

Value<Text> transformReturnValues(TextValue* value);

}// namespace NES::Nautilus
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TEXT_TEXT_HPP_
