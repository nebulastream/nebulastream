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

#ifndef NES_SCHEMABUFFER_HPP
#define NES_SCHEMABUFFER_HPP
#include <DataTypes/PhysicalDataTypes.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::Unikernel {
template<typename Schema, size_t BufferSize>
class SchemaBuffer {
  public:
    using TupleType = Schema::TupleType;

    [[nodiscard]] size_t getSize() const { return buffer.getNumberOfTuples(); }

    [[nodiscard]] constexpr size_t getCapacity() const { return BufferSize / Schema::TupleSize; }

    void writeTuple(Schema::TupleType tuple) {
        size_t offset = buffer.getNumberOfTuples() * Schema::TupleSize;

        // No Overflow
        NES_ASSERT(offset + Schema::TupleSize <= BufferSize, "Out of Bound Write");

        auto tupleMemory = std::span(buffer.getBuffer() + offset, Schema::TupleSize);
        Schema::writeTupleAtBufferAddress(tupleMemory, tuple);
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
    }

    Schema::TupleType readTuple(size_t index) const {
        NES_ASSERT(index < buffer.getNumberOfTuples(), "Out of Bound Read");
        size_t offset = index * Schema::TupleSize;
        // No Overflow
        NES_ASSERT(offset + Schema::TupleSize <= BufferSize, "Out of Bound Read");

        auto tupleMemory = std::span(buffer.getBuffer() + offset, Schema::TupleSize);
        return Schema::readTupleAtBufferAddress(tupleMemory);
    }

    static const SchemaBuffer of(const NES::Runtime::TupleBuffer& tb) {
        // This constructor only hands out a const version of SchemaBuffer which does not allow any modification to tb
        return SchemaBuffer(const_cast<NES::Runtime::TupleBuffer&>(tb));
    }

    NES::Runtime::TupleBuffer getBuffer() { return this->buffer; }

    const NES::Runtime::TupleBuffer getBuffer() const { return this->buffer; }

    static SchemaBuffer of(NES::Runtime::TupleBuffer& tb) { return SchemaBuffer(tb); }

  private:
    explicit SchemaBuffer(Runtime::TupleBuffer& buffer) : buffer(buffer) {}
    NES::Runtime::TupleBuffer& buffer;
};

template<typename T>
struct Field {
    static void write(std::span<uint8_t>, const T::ctype&);
    static T::ctype read(std::span<uint8_t>);
    static constexpr size_t size = T::size;
    using ctype = T::ctype;
};

template<size_t... D, std::size_t... Is>
constexpr std::array<size_t, sizeof...(D) + 1> cumulative_sum(std::index_sequence<Is...>) {
    static_assert(sizeof...(D), "Missing initializers");
    size_t a[]{0, D...};

    for (size_t i = 1; i < size_t(sizeof...(D)) + 1; ++i) {
        a[i] += a[i - 1];
    }
    return {{a[Is]...}};
}

template<size_t... D>
constexpr auto cumulative_sum() {
    return cumulative_sum<D...>(std::make_index_sequence<sizeof...(D) + 1>{});
}

template<typename... T>
struct Schema {

    template<int N, typename... Ts>
    using NthTypeOf = typename std::tuple_element<N, std::tuple<T...>>::type;
    static constexpr std::array<size_t, sizeof...(T) + 1> _array = cumulative_sum<T::size...>();
    using TupleType = std::tuple<typename T::ctype...>;
    static constexpr size_t TupleSize = (T::size + ...);

    static void writeTupleAtBufferAddress(std::span<uint8_t> memoryLocation, TupleType tuple) {
        std::apply(
            [memoryLocation](auto&&... field) {
                size_t i = 0;
                (T::write(memoryLocation.subspan(_array[i++], T::size), field), ...);
            },
            tuple);
    }

    static TupleType readTupleAtBufferAddress(std::span<uint8_t> memoryLocation) {
        size_t i = 0;
        return std::make_tuple((T::read(memoryLocation.subspan(_array[i++], T::size)))...);
    }
};

template<typename T>
inline void Field<T>::write(std::span<uint8_t> memory, const T::ctype& value) {
    NES_ASSERT(memory.size() == T::size, "Memory size does not match");
    NES_ASSERT(reinterpret_cast<std::uintptr_t>(memory.data()) % alignof(typename T::ctype) == 0, "Bad Alignment");

    *reinterpret_cast<T::ctype*>(memory.data()) = value;
}

template<typename T>
inline T::ctype Field<T>::read(std::span<uint8_t> memory) {
    NES_ASSERT(memory.size() == T::size, "Memory size does not match");
    NES_ASSERT(reinterpret_cast<std::uintptr_t>(memory.data()) % alignof(typename T::ctype) == 0, "Bad Alignment");
    return *reinterpret_cast<T::ctype*>(memory.data());
}

//template<>
//inline void Field<TEXT>::write(std::span<uint8_t> memory, const std::string&) {
//    static_assert("Not Implemented");
//    NES_ASSERT(memory.size() == TEXT::size, "Memory size does not match");
//    NES_ASSERT(reinterpret_cast<std::uintptr_t>(memory.data()) % alignof(uint32_t) == 0, "Bad Alignment");
//}
//
//template<>
//std::string Field<TEXT>::read(std::span<uint8_t> memory) {
//    static_assert("Not Implemented");
//    NES_ASSERT(memory.size() == TEXT::size, "Memory size does not match");
//    NES_ASSERT(reinterpret_cast<std::uintptr_t>(memory.data()) % alignof(uint32_t) == 0, "Bad Alignment");
//    return "";
//}
};    // namespace NES::Unikernel
#endif//NES_SCHEMABUFFER_HPP
