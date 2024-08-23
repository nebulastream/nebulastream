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
#pragma once

#include <string>
#include <Runtime/TupleBuffer.hpp>
#include <Common/ExecutableType/BaseVariableSizeType.hpp>
namespace NES::Nautilus
{

/**
 * @brief Physical data type that represents a TextValue.
 * A text value is backed by an tuple buffer.
 * Physical layout:
 * | ----- size 4 byte ----- | ----- variable length char*
 * @note that the char* may have no null termination.
 * Thus when reading char values, we have to check the length attribute. 
 */
class TextValue final : public BaseVariableSizeType
{
public:
    static constexpr size_t DATA_FIELD_OFFSET = sizeof(uint32_t);
    /**
     * @brief Create a new TextValue with a specific size in characters.
     * @param size in characters
     * @return TextValue*
     */
    static TextValue* create(uint32_t size);

    /**
     * @brief Creates a new TextValue from a string
     * @param string content
     * @return TextValue*
     */
    static TextValue* create(const std::string& string);

    /**
     * @brief Creates a new TextValue from a string on a specific tuple buffer
     * @param buffer that is used to create the text
     * @param size in characters
     * @return TextValue*
     */
    static TextValue* create(Memory::TupleBuffer& buffer, uint32_t size);

    /**
     * @brief Creates a new TextValue from a string on a specific tuple buffer
     * @param buffer that is used to create the text
     * @param string content
     * @return TextValue*
     */
    static TextValue* create(Memory::TupleBuffer& buffer, const std::string& string);

    /**
     * @brief Loads a text value from a tuple buffer.
     * The data region of the tuple buffer is reinterpreted as a ListValue.
     * @param string
     * @return TextValue*
     */
    static TextValue* load(Memory::TupleBuffer& tupleBuffer);

    /**
     * @brief Returns the length in the number of characters of the text value
     * @return int32_t
     */
    [[nodiscard]] uint32_t length() const;

    /**
     * @brief Returns the char* to the text.
     * @note char* may not be null terminated.
     * @return char*
     */
    [[nodiscard]] char* str();

    /**
     * @brief Returns the const char* to the text.
     * @note char* may not be null terminated.
     * @return const char*
     */
    [[nodiscard]] const char* c_str() const;

    /**
     * @brief Returns a nullterminated copy of the string
     * @return std::string_view
     */
    [[nodiscard]] std::string strn_copy() const;

    /**
     * @brief Retrieves the underling buffer of this text value.
     * @return Runtime::TupleBuffer
     */
    [[nodiscard]] Memory::TupleBuffer getBuffer() const;

    /**
     * @brief Destructor for the text value that also releases the underling tuple buffer.
     */
    ~TextValue();

private:
    static Memory::TupleBuffer allocateBuffer(uint32_t size);
    /**
     * @brief Private constructor to initialize a new text
     * @param size
     */
    TextValue(uint32_t size);
    const uint32_t size;
};
} /// namespace NES::Nautilus
