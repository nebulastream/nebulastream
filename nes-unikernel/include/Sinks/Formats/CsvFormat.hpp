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

#ifndef NES_CSVFORMAT_HPP
#define NES_CSVFORMAT_HPP
#include <DataTypes/PhysicalDataTypes.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <array>
#include <cstdint>
#include <sstream>
#include <string>
#include <tuple>
namespace NES::Unikernel {

template<typename NESType>
struct Field {
    static void serialize(void* data, const NESType::ctype&);
    static std::string deserialize(const void* data, NES::Runtime::TupleBuffer& tb);
    static constexpr size_t size = NESType::size;
};

template<typename NESType>
void Field<NESType>::serialize(void* data, const typename NESType::ctype& value) {
    *reinterpret_cast<NESType::ctype*>(data) = value;
}

template<>
std::string Field<TEXT>::deserialize(const void* data, NES::Runtime::TupleBuffer& tb) {
    // read the child buffer index from the tuple buffer
    Runtime::TupleBuffer::NestedTupleBufferKey childIdx = *reinterpret_cast<uint32_t const*>(data);

    // retrieve the child buffer from the tuple buffer
    auto childTupleBuffer = tb.loadChildBuffer(childIdx);

    // retrieve the size of the variable-length field from the child buffer
    uint32_t sizeOfTextField = *(childTupleBuffer.getBuffer<uint32_t>());

    // build the string
    if (sizeOfTextField > 0) {
        auto begin = childTupleBuffer.getBuffer() + sizeof(uint32_t);
        return {begin, begin + sizeOfTextField};
    }
    return "";
}

template<typename NESType>
std::string Field<NESType>::deserialize(const void* data, NES::Runtime::TupleBuffer&) {
    return std::to_string(*reinterpret_cast<const NESType::ctype*>(data));
}

template<size_t... D, std::size_t... Is>
constexpr std::array<size_t, sizeof...(D)> cumulative_sum(std::index_sequence<Is...>) {
    static_assert(sizeof...(D), "Missing initializers");
    size_t a[]{D...};
    for (size_t i = 1; i < size_t(sizeof...(D)); ++i) {
        a[i] += a[i - 1];
    }
    return {{a[Is]...}};
}

template<size_t... D>
constexpr auto cumulative_sum() {
    return cumulative_sum<D...>(std::make_index_sequence<sizeof...(D)>{});
}

template<typename... Fields>
struct Schema {
    template<int N, typename... Ts>
    using NthTypeOf = typename std::tuple_element<N, std::tuple<Fields...>>::type;
    static constexpr std::array<size_t, sizeof...(Fields)> _array = cumulative_sum<Fields::size...>();
    static constexpr size_t NumberOfFields = sizeof...(Fields);
    static constexpr size_t TupleSize = _array[NumberOfFields - 1];

    template<size_t index>
    static void serialize(size_t tuple_idx, NthTypeOf<index, Fields...>::ctype value, NES::Runtime::TupleBuffer& tb) {
        NthTypeOf<index, Fields...>::serialize(tb.getBuffer() + _array[index] + (tuple_idx * _array[sizeof...(Fields) - 1]),
                                               value);
    }

    template<typename OUT>
    static void deserialize(OUT& out, NES::Runtime::TupleBuffer& tb) {
        for (size_t idx = 0; idx < tb.getNumberOfTuples(); idx++) {
            size_t base_offset = idx * TupleSize;
            [&tb, &out, base_offset](auto first, auto... rest) {
                int i = 0;
                out << first(static_cast<const void*>(tb.getBuffer() + _array[i++] + base_offset), tb);
                ((out << ", " << rest(static_cast<const void*>(tb.getBuffer() + _array[i++] + base_offset), tb)), ...);
                out << '\n';
            }(Fields::deserialize...);
        }
    }
};

template<typename Schema>
std::string printTupleBufferAsCSV(NES::Runtime::TupleBuffer tbuffer) {
    std::stringstream ss;
    Schema::deserialize(ss, tbuffer);
    return ss.str();
}
};    // namespace NES::Unikernel
#endif//NES_CSVFORMAT_HPP
