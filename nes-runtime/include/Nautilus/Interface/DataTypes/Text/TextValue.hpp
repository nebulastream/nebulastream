#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TEXT_TEXTVALUE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TEXT_TEXTVALUE_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <string>
namespace NES::Nautilus {

/**
 * @brief Physical data type that represents a TextValue.
 * A text value is backed by an tuple buffer.
 * Physical layout:
 * | ----- size 4 byte ----- | ----- variable length char*
 */
class TextValue final {
  public:
    static constexpr size_t DATA_FIELD_OFFSET = sizeof(int32_t);
    static TextValue* create(int32_t size);
    static TextValue* create(const std::string& string);
    static TextValue* load(Runtime::TupleBuffer& tupleBuffer);

    /**
     * @brief Returns the length in the number of characters of the text value
     * @return int32_t
     */
    int32_t length() const;

    /**
     * @brief Returns the char* to the text.
     * @return char*
     */
    char* str();

    /**
     * @brief Returns the const char* to the text.
     * @return const char*
     */
    const char* c_str() const;

    /**
     * @brief Destructor for the text value that also releases the underling tuple buffer.
     */
    ~TextValue();

  private:
    /**
     * @brief Private constructor to initialize a new text
     * @param size
     */
    TextValue(int32_t size);
    const int32_t size;
};
}// namespace NES::Nautilus
#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TEXT_TEXTVALUE_HPP_
