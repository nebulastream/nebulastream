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

#ifndef NES_DATA_PARSER_INCLUDE_GENERATION_SCHEMABUFFER_HPP_
#define NES_DATA_PARSER_INCLUDE_GENERATION_SCHEMABUFFER_HPP_

#include <Runtime/TupleBuffer.hpp>
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <generation/TypeMapping.hpp>
#include <generation/concepts.hpp>
#include <span>
#include <tuple>
#include <utility>

#define SPAN std::span

namespace NES::DataParser {

namespace detail {

/**
 * Template wrapper type for string like literals as template parameter. (e.g. Field<Type, "Name">)
 * @tparam N CTAD infers the template parameter for the size
 */
template<size_t N>
struct StringLiteral {
    constexpr StringLiteral(const char (&str)[N]) { std::copy_n(str, N, value); }

    char value[N];
};

// Helper to identifiy duplicated StringLiterals in a template pack
template<StringLiteral Head, StringLiteral... Tail>
constexpr bool HasDuplicateOccurenceOfHead = ((Head.value == Tail.value) || ...);

template<StringLiteral... V>
struct UniqueNames;

template<StringLiteral H1, StringLiteral H2, StringLiteral... Tail>
struct UniqueNames<H1, H2, Tail...> {
    constexpr static bool value = !HasDuplicateOccurenceOfHead<H1, H2, Tail...> && UniqueNames<H2, Tail...>::value;
};

template<StringLiteral... Tail>
    requires(sizeof...(Tail) <= 1)
struct UniqueNames<Tail...> {
    constexpr static bool value = true;
};
static_assert(UniqueNames<"A", "B", "C">::value);
static_assert(!UniqueNames<"A", "B", "A">::value);
static_assert(!UniqueNames<"A", "B", "B">::value);
static_assert(!UniqueNames<"A", "A">::value);
static_assert(UniqueNames<>::value);
static_assert(UniqueNames<"A">::value);

/**
 * Returns the offset in bytes for a specific std::tuple element to the base address of the tuple.
 * @tparam I index of the field
 * @tparam Tuple tuple type (e.g. std::tuple<int, float, int>)
 * @return offset in bytes
 */
template<std::size_t I, typename Tuple>
constexpr std::size_t tupleElementOffset() {
    using element_t = std::tuple_element_t<I, Tuple>;
    static_assert(!std::is_reference_v<element_t>);
    union {
        char a[sizeof(Tuple)];
        Tuple t{};
    };
    auto* p = std::addressof(std::get<I>(t));
    t.~Tuple();
    for (std::size_t i = 0;; ++i) {
        if (static_cast<void*>(a + i) == p)
            return i;
    }
}
template<typename... T, std::size_t... Is>
constexpr std::array<size_t, sizeof...(T)> fieldOffsets(std::index_sequence<Is...>) {
    size_t a[sizeof...(T)]{};
    ((a[Is] = tupleElementOffset<Is, std::tuple<T...>>()), ...);
    return {{a[Is]...}};
}
template<typename... Ts>
constexpr std::array<size_t, sizeof...(Ts)> fieldOffsets() {
    return fieldOffsets<Ts...>(std::make_index_sequence<sizeof...(Ts)>{});
}

// specific values are compiler / library dependend, but results might look like this
static_assert(fieldOffsets<uint32_t, double, uint32_t, double, std::string_view>() == std::array<size_t, 5>{40, 32, 24, 16, 0});
}// namespace detail

/**
 * Templated type serializer. Concrete specializations know how to write a C-Type to a memory region
 * @tparam T Schema Field Type
 */
template<Concepts::Type T>
struct TypeToBufferSerializer {
    static void write(SPAN<uint8_t>, const typename T::CType&, NES::Runtime::TupleBuffer&);
};

/**
 * Generic write implementation for all trivally copyable types (ints, floats)
 * @param memory memory segment of the TupleBuffer for the fields value
 * @param value plain c++ value
 */
template<Concepts::Type T>
void TypeToBufferSerializer<T>::write(std::span<uint8_t> memory, const typename T::CType& value, NES::Runtime::TupleBuffer&) {
    // NES_ASSERT(memory.size() == T::Size, "Memory size does not match");
    std::memcpy(memory.data(), &value, sizeof(typename T::CType));
}

/**
 * @brief Specialization for saving std::string_views. Data is copied into a new ChildBuffer and linked to the TupleBuffer
 * @param memory memory segment of the TupleBuffer for the ChildIndex
 * @param value string_view value
 * @param tb target TupleBuffer
 */
template<>
inline void
TypeToBufferSerializer<TEXT>::write(std::span<uint8_t> memory, const std::string_view& value, NES::Runtime::TupleBuffer& tb) {
    // NES_ASSERT(memory.size() == TEXT::Size, "Memory size does not match");

    //allocate child buffer
    auto textBuffer = new uint8_t[sizeof(uint32_t) + value.size()];
    std::memcpy(textBuffer + sizeof(uint32_t), value.data(), value.size());
    auto childBuffer = NES::Runtime::TupleBuffer::wrapMemory(textBuffer, sizeof(uint32_t) + value.size(), [](auto segment, auto) {
        delete[] segment->getPointer();
        delete segment;
    });
    *childBuffer.getBuffer<uint32_t>() = value.size();

    auto childBufferIndex = tb.storeChildBuffer(childBuffer);
    std::memcpy(memory.data(), &childBufferIndex, sizeof(childBufferIndex));
}

/**
 * @brief FieldSerializer knows how to serialize a Tuples field to a memory region. Currently this class delegates to the
 * underlying types serializer, however future implementation might make use of the additional fields name.
 * @tparam Type
 */
template<Concepts::Type Type>
class FieldSerializer {
  public:
    static void write(SPAN<uint8_t> fieldDataSegment, const typename Type::CType& value, NES::Runtime::TupleBuffer& buffer) {
        TypeToBufferSerializer<Type>::write(fieldDataSegment, value, buffer);
    }
};

template<typename Tuple>
class TupleSerializer;

template<Concepts::Field... Fields>
class TupleSerializer<std::tuple<Fields...>> {
    // Computes the cumulated offsets of each field in the tuple
    // if we have fields of size [2, 4, 2, 1, 8, 8] the result would be [0, 2, 6, 8, 9, 17, 25]
    template<size_t... D, std::size_t... Is>
    static constexpr std::array<size_t, sizeof...(D) + 1> cumulative_sum(std::index_sequence<Is...>) {
        size_t a[]{0, D...};
        for (size_t i = 1; i < size_t(sizeof...(D)) + 1; ++i) {
            a[i] += a[i - 1];
        }
        return {{a[Is]...}};
    }
    template<size_t... D>
    static constexpr auto cumulative_sum() {
        return cumulative_sum<D...>(std::make_index_sequence<sizeof...(D) + 1>{});
    }
    static_assert(cumulative_sum<2, 4, 2, 1, 8, 8>() == std::array<size_t, 7>{0, 2, 6, 8, 9, 17, 25}, "Cumulative Sum Error");

    using TupleType = std::tuple<typename Fields::Type::CType...>;
    static constexpr std::array<size_t, sizeof...(Fields) + 1> fieldOffsetsInTupleBuffer = cumulative_sum<Fields::Size...>();

  public:
    constexpr static size_t TupleSize = (0 + ... + Fields::Type::Size);
    static void writeTupleAtBufferAddress(SPAN<uint8_t> memoryLocation, TupleType tuple, Runtime::TupleBuffer& tb) {
        // Write each field of the tuple into its designated location within the memoryLocation for the entire tuple
        // the fields memory location is advanced by the precomputed fieldOffsetsInTupleBuffer array
        std::apply(
            [&](auto... tupleField) {
                size_t i = 0;
                (FieldSerializer<typename Fields::Type>::write(
                     memoryLocation.subspan(fieldOffsetsInTupleBuffer[i++], Fields::Size),
                     tupleField,
                     tb),
                 ...);
            },
            tuple);
    }
};

template<Concepts::Type T, detail::StringLiteral name>
struct Field {
    using Type = T;
    constexpr static size_t Size = T::Size;
    constexpr static detail::StringLiteral NameLiteral = name;
    constexpr static const char* Name = name.value;
};

template<Concepts::Field... F>
struct StaticSchema {
    // This check ensures that a StaticSchema does not contain duplicate field names
    static_assert(detail::UniqueNames<F::NameLiteral...>::value, "Field names have to be unique");

    using Fields = std::tuple<F...>;
    using TupleType = std::tuple<typename F::Type::CType...>;
    static constexpr std::array Offsets = detail::fieldOffsets<typename F::Type::CType...>();
};

template<Concepts::Schema S, size_t BS>
struct SchemaBuffer {
    using Schema = S;
    using Serializer = TupleSerializer<typename S::Fields>;
    constexpr static size_t BufferSize = BS;
    constexpr static size_t Capacity = BufferSize / Serializer::TupleSize;

    [[nodiscard]] size_t getSize() const { return buffer.getNumberOfTuples(); }
    [[nodiscard]] bool isFull() const { return Capacity == getSize(); }
    [[nodiscard]] bool isEmpty() const { return getSize() == 0; }

    void writeTuple(typename S::TupleType tuple) {
        size_t offset = buffer.getNumberOfTuples() * Serializer::TupleSize;
        auto tupleMemory = SPAN(buffer.getBuffer() + offset, Serializer::TupleSize);
        Serializer::writeTupleAtBufferAddress(tupleMemory, tuple, buffer);
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
    }

    explicit SchemaBuffer(Runtime::TupleBuffer& buffer) : buffer(buffer) {}

  private:
    Runtime::TupleBuffer& buffer;
};

// Testing
static_assert(NES::DataParser::Concepts::Type<UINT8>);
static_assert(NES::DataParser::Concepts::Field<Field<UINT8, "a">>);
static_assert(Field<UINT8, "Byte">::Size == 1);
static_assert(NES::DataParser::Concepts::Schema<StaticSchema<Field<FLOAT64, "a">, Field<FLOAT32, "b">>>);
static_assert(NES::DataParser::Concepts::SchemaBuffer<SchemaBuffer<StaticSchema<Field<FLOAT32, "a">, Field<INT64, "b">>, 8192>>);
static_assert(SchemaBuffer<StaticSchema<Field<INT32, "a">, Field<UINT32, "b">, Field<UINT64, "c">>, 8192>::Capacity == 512);
}// namespace NES::DataParser
#endif// NES_DATA_PARSER_INCLUDE_GENERATION_SCHEMABUFFER_HPP_
