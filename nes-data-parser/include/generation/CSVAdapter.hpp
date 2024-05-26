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

#ifndef NES_DATA_PARSER_INCLUDE_GENERATION_CSVADAPTER_HPP_
#define NES_DATA_PARSER_INCLUDE_GENERATION_CSVADAPTER_HPP_

#include <cstddef>
#include <generation/SchemaBuffer.hpp>
#include <scn/scan.h>
#include <tuple>

namespace NES::DataParser::CSV {
namespace detail {

template<char... symbols>
struct String {
    static constexpr char value[] = {symbols...};
};

template<typename, typename>
struct Concat;

template<char... symbols1, char... symbols2>
struct Concat<String<symbols1...>, String<symbols2...>> {
    using type = String<symbols1..., symbols2...>;
};

template<typename...>
struct Concatenate;

template<typename S, typename... Strings>
struct Concatenate<S, Strings...> {
    using type = typename Concat<S, typename Concatenate<Strings...>::type>::type;
};

template<>
struct Concatenate<> {
    using type = String<>;
};

template<typename Separtor, typename...>
struct ConcatPlaceholdersRec;

template<typename Separator, typename S, typename... Strings>
struct ConcatPlaceholdersRec<Separator, S, Strings...> {
    using type = typename Concatenate<Separator, S, typename ConcatPlaceholdersRec<Separator, Strings...>::type>::type;
};

template<typename Separator>
struct ConcatPlaceholdersRec<Separator> {
    using type = String<'\n', '\0'>;
};

template<typename Separator, typename S, typename... Strings>
struct ConcatPlaceholders {
    using type = typename Concatenate<S, typename ConcatPlaceholdersRec<Separator, Strings...>::type>::type;
};

using DefaultScanPlaceholder = String<'{', '}'>;
using Comma = String<','>;

template<Concepts::Type T>
struct CSVFormatAdapter {
    using ScanLibPlaceHolder = DefaultScanPlaceholder;
};

template<>
struct CSVFormatAdapter<TEXT> {
    //Generates \"{:[^\"]}\", which matches a string in quotes
    using ScanLibPlaceHolder = String<'\"', '{', ':', '[', '^', '\"', ']', '}', '\"'>;
};

template<Concepts::Field... Fields>
struct CSVSchemaFormat {
    static constexpr const char* fmt =
        ConcatPlaceholders<Comma, typename CSVFormatAdapter<typename Fields::Type>::ScanLibPlaceHolder...>::type::value;
};
}// namespace detail

template<NES::DataParser::Concepts::SchemaBuffer SB>
struct CSVAdapter {

    constexpr static const char* format = std::apply(
        []<NES::DataParser::Concepts::Field... F>(F...) {
            return detail::CSVSchemaFormat<F...>::fmt;
        },
        typename SB::Schema::Fields{});

    template<typename... T>
    static std::string_view scanRecursively(std::string_view sv, NES::Runtime::TupleBuffer& b, size_t tuplesLeft) {
        if (tuplesLeft == 0) {
            return sv;
        }

        if (auto result = scn::scan<T...>(sv, format)) {
            SB(b).writeTuple(result->values());
            auto new_sv = std::string_view{result->range().begin(), result->range().end()};
            return CSVAdapter::scanRecursively<T...>(new_sv, b, tuplesLeft - 1);
        }
        return sv;
    }

    static std::string_view parseIntoBuffer(SPAN<const char> data, NES::Runtime::TupleBuffer& buffer) {
        size_t tuplesLeft = SB::Capacity - buffer.getNumberOfTuples();
        // size_t bytesConsumed = 0;
        return std::apply(
            [data, &buffer, tuplesLeft]<typename... T>(T...) {
                return scanRecursively<T...>(std::string_view{data.begin(), data.end()}, buffer, tuplesLeft);
            },
            typename SB::Schema::TupleType{});
    }

    static std::pair<std::string_view, typename SB::Schema::TupleType> parseAndAdvance(std::string_view data) {
        auto result = std::apply(
            [data]<typename... T>(T...) {
                return scn::scan<T...>(data, format);
            },
            typename SB::Schema::TupleType{});

        if (result) {
            auto new_sv = std::string_view{result->range().begin(), result->range().end()};
            return {new_sv, result->values()};
        }

        return {data, typename SB::Schema::TupleType{}};
    }
};

}// namespace NES::DataParser::CSV
#endif// NES_DATA_PARSER_INCLUDE_GENERATION_CSVADAPTER_HPP_
