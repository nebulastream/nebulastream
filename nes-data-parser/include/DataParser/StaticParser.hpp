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

#ifndef NES_DATA_PARSER_INCLUDE_DATAPARSER_STATICPARSER_HPP_
#define NES_DATA_PARSER_INCLUDE_DATAPARSER_STATICPARSER_HPP_
#include <DataParser/Parser.hpp>
#include <Runtime/FormatType.hpp>
#include <generation/CSVAdapter.hpp>
#include <generation/JSONAdapter.hpp>
namespace NES::DataParser {

/**
 * Parser implementation for statically known Schemas. If the schema is only known at runtime (e.g. using NES::SchemaPtr) the
 * `ParserFactory` can generate Parser implementation
 * @tparam S StaticSchema
 * @tparam F FormatType
 */
template<Concepts::Schema S, FormatTypes F>
class StaticParser final : public Parser {
    static thread_local typename S::TupleType tuple;
    // Somewhat messy way of limiting static_asserts to if-constexpr branches
    template<FormatTypes...>
    static constexpr std::false_type always_false{};

  public:
    [[nodiscard]] std::string_view parseIntoBuffer(std::string_view data, Runtime::TupleBuffer& buffer) const override {
        if constexpr (F == FormatTypes::CSV_FORMAT) {
            return CSV::CSVAdapter<SchemaBuffer<S, 8192>>::parseIntoBuffer(data, buffer);
        } else if constexpr (F == FormatTypes::JSON_FORMAT) {
            return JSON::JSONAdapter<SchemaBuffer<S, 8192>>::parseIntoBuffer(data, buffer);
        } else {
            static_assert(always_false<F>, "Format is not implemented");
        }
    }

    [[nodiscard]] std::vector<void*> getFieldOffsets() const override {
        std::vector<void*> results;
        for (auto offset : S::Offsets) {
            results.push_back(static_cast<void*>(reinterpret_cast<std::byte*>(std::addressof(tuple)) + offset));
        }
        return results;
    }

    [[nodiscard]] std::string_view parse(std::string_view view) const override {
        std::string_view left;
        if constexpr (F == FormatTypes::CSV_FORMAT) {
            std::tie(left, tuple) = CSV::CSVAdapter<SchemaBuffer<S, 8192>>::parseAndAdvance(view);
        } else if constexpr (F == FormatTypes::JSON_FORMAT) {
            std::tie(left, tuple) = JSON::JSONAdapter<SchemaBuffer<S, 8192>>::parseAndAdvance(view);
        }
        return left;
    }
};
template<Concepts::Schema S, FormatTypes F>
thread_local typename S::TupleType StaticParser<S, F>::tuple = {};
}// namespace NES::DataParser
#endif// NES_DATA_PARSER_INCLUDE_DATAPARSER_STATICPARSER_HPP_
