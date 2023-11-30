/*
* MIT License
*
* Copyright(c) 2018 Jimmie Bergmann
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files(the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions :
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*
*/

/*
YAML documentation:
http://yaml.org/spec/1.0/index.html
https://www.codeproject.com/Articles/28720/YAML-Parser-in-C
*/

#ifndef NES_COMMON_INCLUDE_UTIL_YAML_YAML_HPP_
#define NES_COMMON_INCLUDE_UTIL_YAML_YAML_HPP_

#include <algorithm>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

/**
 * @brief Namespace wrapping mini-yaml classes.
 */
namespace Yaml {

constexpr uint32_t YAML_UNSPECIFIED_INTEGER = 32759;

/**
 * @brief Forward declarations.
 */
class Node;

/**
 * @brief Helper classes and functions
 */
namespace impl {

/**
 * @brief Helper functionality, converting string to any data type.
 *        Strings are left untouched.
 */
template<typename T>
struct StringConverter {
    static T Get(const std::string& data) {
        T type;
        std::stringstream ss(data);
        ss >> type;
        return type;
    }

    static T Get(const std::string& data, const T& defaultValue) {
        T type;
        std::stringstream ss(data);
        ss >> type;

        if (ss.fail()) {
            return defaultValue;
        }

        return type;
    }
};
template<>
struct StringConverter<std::string> {
    static std::string Get(const std::string& data) { return data; }

    static std::string Get(const std::string& data, const std::string& defaultValue) {
        if (data.empty()) {
            return defaultValue;
        }
        return data;
    }
};

template<>
struct StringConverter<bool> {
    static bool Get(const std::string& data) {
        std::string tmpData = data;
        std::transform(tmpData.begin(), tmpData.end(), tmpData.begin(), ::tolower);
        return tmpData == "true" || tmpData == "yes" || tmpData == "1";
    }

    static bool Get(const std::string& data, const bool& defaultValue) {
        if (data.empty()) {
            return defaultValue;
        }

        return Get(data);
    }
};

}// namespace impl

/**
    * @brief Exception class.
    *
    */
class Exception : public std::runtime_error {

  public:
    /**
        * @brief Enumeration of exception types.
        *
        */
    enum class eType : uint8_t {
        InternalError,///< Internal error.
        ParsingError, ///< Invalid parsing data.
        OperationError///< User operation error.
    };

    /**
        * @brief Constructor.
        *
        * @param message    Exception message.
        * @param type       Type of exception.
        *
        */
    Exception(const std::string& message, eType type);

    /**
        * @brief Get type of exception.
        *
        */
    [[nodiscard]] eType Type() const;

    /**
        * @brief Get message of exception.
        *
        */
    [[nodiscard]] const char* Message() const;

  private:
    eType m_Type;///< Type of exception.
};

/**
    * @brief Internal exception class.
    *
    * @see Exception
    *
    */
class InternalException : public Exception {

  public:
    /**
        * @brief Constructor.
        *
        * @param message Exception message.
        *
        */
    explicit InternalException(const std::string& message);
};

/**
    * @brief Parsing exception class.
    *
    * @see Exception
    *
    */
class ParsingException : public Exception {

  public:
    /**
        * @brief Constructor.
        *
        * @param message Exception message.
        *
        */
    explicit ParsingException(const std::string& message);
};

/**
    * @brief Operation exception class.
    *
    * @see Exception
    *
    */
class OperationFatalException : public Exception {

  public:
    /**
        * @brief Constructor.
        *
        * @param message Exception message.
        *
        */
    explicit OperationFatalException(const std::string& message);
};

/**
    * @brief Iterator class.
    *
    */
class Iterator {

  public:
    friend class Node;

    /**
        * @brief Default constructor.
        *
        */
    Iterator();

    /**
        * @brief Copy constructor.
        *
        */
    Iterator(const Iterator& it);

    /**
        * @brief Assignment operator.
        *
        */
    Iterator& operator=(const Iterator& it);

    /**
        * @brief Destructor.
        *
        */
    ~Iterator();

    /**
        * @brief Get node of iterator.
        *        First pair item is the key of map value, empty if type is sequence.
        *
        */
    std::pair<const std::string&, Node&> operator*();

    /**
        * @brief Post-increment operator.
        *
        */
    Yaml::Iterator operator++(int);

    /**
        * @brief Post-decrement operator.
        *
        */
    Yaml::Iterator operator--(int);

    /**
        * @brief Check if iterator is equal to other iterator.
        *
        */
    bool operator==(const Iterator& it) const;

    /**
        * @brief Check if iterator is not equal to other iterator.
        *
        */
    bool operator!=(const Iterator& it) const;

  private:
    enum class eType : int8_t { None, SequenceType, MapType };

    eType m_Type{eType::None};///< Type of iterator.
    void* m_pImp{nullptr};    ///< Implementation of iterator class.
};

/**
    * @brief Constant iterator class.
    *
    */
class ConstIterator {

  public:
    friend class Node;

    /**
        * @brief Default constructor.
        *
        */
    ConstIterator();

    /**
        * @brief Copy constructor.
        *
        */
    ConstIterator(const ConstIterator& it);

    /**
        * @brief Assignment operator.
        *
        */
    ConstIterator& operator=(const ConstIterator& it);

    /**
        * @brief Destructor.
        *
        */
    ~ConstIterator();

    /**
        * @brief Get node of iterator.
        *        First pair item is the key of map value, empty if type is sequence.
        *
        */
    std::pair<const std::string&, const Node&> operator*();

    /**
        * @brief Post-increment operator.
        *
        */
    Yaml::ConstIterator operator++(int);

    /**
        * @brief Post-decrement operator.
        *
        */
    Yaml::ConstIterator operator--(int);

    /**
        * @brief Check if iterator is equal to other iterator.
        *
        */
    friend bool operator==(const ConstIterator& lhs, const ConstIterator& rhs);

    /**
        * @brief Check if iterator is not equal to other iterator.
        *
        */
    friend bool operator!=(const ConstIterator& lhs, const ConstIterator& rhs);

  private:
    enum class eType : int8_t { None, SequenceType, MapType };

    eType m_Type{eType::None};///< Type of iterator.
    void* m_pImp{nullptr};    ///< Implementation of constant iterator class.
};

/**
    * @brief Node class.
    *
    */
class Node {

  public:
    friend class Iterator;

    /**
        * @brief Enumeration of node types.
        *
        */
    enum class eType : int8_t { None, SequenceType, MapType, ScalarType };

    /**
        * @brief Default constructor.
        *
        */
    Node();

    /**
        * @brief Copy constructor.
        *
        */
    Node(const Node& node);

    /**
        * @brief Assignment constructors.
        *        Converts node to scalar type if needed.
        *
        */
    explicit Node(const std::string& value);
    explicit Node(const char* value);

    /**
        * @brief Destructor.
        *
        */
    ~Node();

    /**
        * @brief Functions for checking type of node.
        *
        */
    [[nodiscard]] eType Type() const;
    [[nodiscard]] bool IsNone() const;
    [[nodiscard]] bool IsSequence() const;
    [[nodiscard]] bool IsMap() const;
    [[nodiscard]] bool IsScalar() const;

    /**
        * @brief Completely clear node.
        *
        */
    void Clear();

    /**
        * @brief Get node as given template type.
        *
        */
    template<typename T>
    [[nodiscard]] T As() const {
        return impl::StringConverter<T>::Get(AsString());
    }

    /**
        * @brief Get node as given template type.
        *
        */
    template<typename T>
    T As(const T& defaultValue) const {
        return impl::StringConverter<T>::Get(AsString(), defaultValue);
    }

    /**
        * @brief Get size of node.
        *        Serialization of type None or Scalar will return 0.
        *
        */
    [[nodiscard]] size_t Size() const;

    // Sequence operators

    /**
        * @brief Insert sequence item at given index.
        *        Converts node to sequence type if needed.
        *        Adding new item to end of sequence if index is larger than sequence size.
        *
        */
    Node& Insert(size_t index);

    /**
        * @brief Add new sequence index to back.
        *        Converts node to sequence type if needed.
        *
        */
    Node& PushFront();

    /**
        * @brief Add new sequence index to front.
        *        Converts node to sequence type if needed.
        *
        */
    Node& PushBack();

    /**
        * @brief    Get sequence/map item.
        *           Converts node to sequence/map type if needed.
        *
        * @param index  Sequence index. Returns None type Node if index is unknown.
        * @param key    Map key. Creates a new node if key is unknown.
        *
        */
    Node& operator[](size_t index);
    Node& operator[](const std::string& key);

    /**
        * @brief Erase item.
        *        No action if node is not a sequence or map.
        *
        */
    void Erase(size_t index);
    void Erase(const std::string& key);

    /**
        * @brief Assignment operators.
        *
        */
    Node& operator=(const Node& node);
    Node& operator=(const std::string& value);
    Node& operator=(const char* value);

    /**
        * @brief Get start iterator.
        *
        */
    Iterator Begin();
    [[nodiscard]] ConstIterator Begin() const;

    /**
        * @brief Get end iterator.
        *
        */
    Iterator End();
    [[nodiscard]] ConstIterator End() const;

  private:
    /**
        * @brief Get as string. If type is scalar, else empty.
        *
        */
    [[nodiscard]] const std::string& AsString() const;

    void* m_pImp;///< Implementation of node class.
};

/**
    * @brief Parsing functions.
    *        Population given root node with deserialized data.
    *
    * @param root       Root node to populate.
    * @param filename   Path of input file.
    * @param stream     Input stream.
    * @param string     String of input data.
    * @param buffer     Char array of input data.
    * @param size       Buffer size.
    *
    * @throw InternalException  An internal error occurred.
    * @throw ParsingException   Invalid input YAML data.
    * @throw OperationFatalException If filename or buffer pointer is invalid.
    *
    */
void Parse(Node& root, const char* filename);
void Parse(Node& root, std::iostream& stream);
void Parse(Node& root, const std::string& string);
void Parse(Node& root, const char* buffer, size_t size);

/**
    * @brief    Serialization configuration structure,
    *           describing output behavior.
    *
    */
struct SerializeConfig {

    /**
        * @brief Constructor.
        *
        * @param spaceIndentation       Number of spaces per indentation.
        * @param scalarMaxLength        Maximum length of scalars. Serialized as folder scalars if exceeded.
        *                               Ignored if equal to 0.
        * @param sequenceMapNewline     Put maps on a new line if parent node is a sequence.
        * @param mapScalarNewline       Put scalars on a new line if parent node is a map.
        *
        */
    explicit SerializeConfig(size_t spaceIndentation = 2,
                             size_t scalarMaxLength = 64,
                             bool sequenceMapNewline = false,
                             bool mapScalarNewline = false);

    size_t SpaceIndentation;///< Number of spaces per indentation.
    size_t ScalarMaxLength; ///< Maximum length of scalars. Serialized as folder scalars if exceeded.
    bool SequenceMapNewline;///< Put maps on a new line if parent node is a sequence.
    bool MapScalarNewline;  ///< Put scalars on a new line if parent node is a map.
};

/**
    * @brief Serialization functions.
    *
    * @param root       Root node to serialize.
    * @param filename   Path of output file.
    * @param stream     Output stream.
    * @param string     String of output data.
    * @param config     Serialization configurations.
    *
    * @throw InternalException  An internal error occurred.
    * @throw OperationFatalException If filename or buffer pointer is invalid.
    *                           If config is invalid.
    *
    */
void Serialize(Node const& root, char const* filename, SerializeConfig const& config);
void Serialize(Node const& root, std::iostream& stream, SerializeConfig const& config);
void Serialize(Node const& root, std::string& string, SerializeConfig const& config);

}// namespace Yaml
#endif// NES_COMMON_INCLUDE_UTIL_YAML_YAML_HPP_
