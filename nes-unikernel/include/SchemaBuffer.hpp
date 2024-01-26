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
#include <span>
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
        Schema::writeTupleAtBufferAddress(tupleMemory, tuple, buffer);
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
    }

    Schema::TupleType readTuple(size_t index) const {
        NES_ASSERT(index < buffer.getNumberOfTuples(), "Out of Bound Read");
        size_t offset = index * Schema::TupleSize;
        // No Overflow
        NES_ASSERT(offset + Schema::TupleSize <= BufferSize, "Out of Bound Read");

        auto tupleMemory = std::span(buffer.getBuffer() + offset, Schema::TupleSize);
        return Schema::readTupleAtBufferAddress(tupleMemory, buffer);
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
    static void write(std::span<uint8_t>, const T::ctype&, NES::Runtime::TupleBuffer&);
    static T::ctype read(std::span<uint8_t>, NES::Runtime::TupleBuffer&);
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

    static void writeTupleAtBufferAddress(std::span<uint8_t> memoryLocation, TupleType tuple, NES::Runtime::TupleBuffer& tb) {
        std::apply(
            [memoryLocation, &tb](auto&&... field) {
                size_t i = 0;
                (T::write(memoryLocation.subspan(_array[i++], T::size), field, tb), ...);
            },
            tuple);
    }

    static TupleType readTupleAtBufferAddress(std::span<uint8_t> memoryLocation, NES::Runtime::TupleBuffer& tb) {
        size_t i = 0;
        return std::make_tuple((T::read(memoryLocation.subspan(_array[i++], T::size), tb))...);
    }
};

template<typename T>
void Field<T>::write(std::span<uint8_t> memory, const typename T::ctype& value, NES::Runtime::TupleBuffer&) {
    NES_ASSERT(memory.size() == T::size, "Memory size does not match");
    std::memcpy(memory.data(), &value, sizeof(typename T::ctype));
}

template<typename T>
typename T::ctype Field<T>::read(std::span<uint8_t> memory, NES::Runtime::TupleBuffer&) {
    NES_ASSERT(memory.size() == T::size, "Memory size does not match");
    typename T::ctype value;
    std::memcpy(&value, memory.data(), sizeof(typename T::ctype));
    return value;
}

template<>
inline void Field<TEXT>::write(std::span<uint8_t> memory, const std::string& value, NES::Runtime::TupleBuffer& tb) {
    NES_ASSERT(memory.size() == TEXT::size, "Memory size does not match");

    //allocate child buffer
    auto textBuffer = new uint8_t[sizeof(uint32_t) + value.size()];
    std::memcpy(textBuffer + sizeof(uint32_t), value.c_str(), value.size());
    auto childBuffer = NES::Runtime::TupleBuffer::wrapMemory(textBuffer, sizeof(uint32_t) + value.size(), [](auto segment, auto) {
        delete segment->getPointer();
        delete segment;
    });
    *childBuffer.getBuffer<uint32_t>() = value.size();

    auto childBufferIndex = tb.storeChildBuffer(childBuffer);
    std::memcpy(memory.data(), &childBufferIndex, sizeof(childBufferIndex));
}

template<>
std::string Field<TEXT>::read(std::span<uint8_t> memory, NES::Runtime::TupleBuffer& tb) {
    NES_ASSERT(memory.size() == TEXT::size, "Memory size does not match");

    int32_t childBufferIndex = 0;
    std::memcpy(&childBufferIndex, memory.data(), sizeof(childBufferIndex));

    auto childBuffer = tb.loadChildBuffer(childBufferIndex);
    auto textSize = *childBuffer.getBuffer<uint32_t>();

    return {childBuffer.getBuffer<char>() + sizeof(textSize), textSize};
}
};    // namespace NES::Unikernel
#endif//NES_SCHEMABUFFER_HPP
