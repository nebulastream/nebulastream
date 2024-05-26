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

#ifndef NES_DATA_PARSER_INCLUDE_GENERATION_CONCEPTS_HPP_
#define NES_DATA_PARSER_INCLUDE_GENERATION_CONCEPTS_HPP_
namespace NES::DataParser::Concepts {

template<int>
using helper = void;

template<typename T>
concept Type = requires(T f) {
    typename T::CType;

    T::Size;
    std::same_as<decltype(T::Size), size_t>;
    typename helper<T::Size>;

    T::NESType;
    std::same_as<decltype(T::NESType), NES::BasicType>;
};

template<typename F>
concept Field = requires(F f) {
    typename F::Type;

    F::Size;
    std::same_as<decltype(F::Size), size_t>;
    typename helper<F::Size>;

    F::Name;
    std::same_as<decltype(F::Name), const char*>;
};

template<typename T, typename... Fs>
concept Schema = requires(T t, Fs... fs) {
    typename T::TupleType;
    std::same_as<typename T::TupleType, std::tuple<typename Fs::CType...>>;

    typename T::Fields;
};

template<typename T>
concept SchemaBuffer = requires(T schemaBuffer) {
    T::Capacity;
    std::same_as<decltype(T::Capacity), size_t>;
    typename helper<T::Capacity>;

    typename T::Schema;

    T::BufferSize;
    std::same_as<decltype(T::BufferSize), size_t>;
    typename helper<T::BufferSize>;
};
}// namespace NES::DataParser::Concepts
#endif// NES_DATA_PARSER_INCLUDE_GENERATION_CONCEPTS_HPP_
