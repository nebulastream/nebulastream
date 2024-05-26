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

#ifndef NES_DATA_PARSER_INCLUDE_GENERATION_JSONADAPTER_HPP_
#define NES_DATA_PARSER_INCLUDE_GENERATION_JSONADAPTER_HPP_

#include <cstddef>
#include <generation/SchemaBuffer.hpp>
#include <third_party/hashes.hh>
#include <third_party/phf.hh>
#include <third_party/simdjson.h>
#include <tuple>

#define SIMDJSON_ERROR(err)                                                                                                      \
    do {                                                                                                                         \
        auto __error = (err);                                                                                                    \
        if (__error) {                                                                                                           \
            std::cerr << __error << std::endl;                                                                                   \
            std::exit(1);                                                                                                        \
        }                                                                                                                        \
    } while (0)

namespace NES::DataParser::JSON {
namespace detail {

template<typename T>
void parse_into(simdjson::ondemand::value& value, T& t);

template<std::floating_point FP>
void parse_into(simdjson::ondemand::value& value, FP& t) {
    double v;
    SIMDJSON_ERROR(value.get_double().get(v));
    t = static_cast<FP>(v);
}
template<std::unsigned_integral UnsignedInt>
void parse_into(simdjson::ondemand::value& value, UnsignedInt& t) {
    uint64_t u;
    SIMDJSON_ERROR(value.get_uint64().get(u));
    t = static_cast<UnsignedInt>(u);
}
template<std::signed_integral SignedInt>
void parse_into(simdjson::ondemand::value& value, SignedInt& t) {
    int64_t i;
    SIMDJSON_ERROR(value.get_int64().get(i));
    t = static_cast<SignedInt>(i);
}
template<>
inline void parse_into(simdjson::ondemand::value& value, std::string_view& t) {
    auto v = value.raw_json_token();
    t = {v.data() + 1, v.find_last_of('"') - 1};
}

template<class T, T... Is, class F>
void compile_switch(T i, std::integer_sequence<T, Is...>, F f) {
    std::initializer_list<int>({(i == Is ? (f(std::integral_constant<T, Is>{})), 0 : 0)...});
}

template<class T, std::size_t... Is>
void func_impl(std::size_t counter, simdjson::ondemand::value& value, T& t, std::index_sequence<Is...> is) {
    compile_switch(counter, is, [&](auto i) {
        parse_into(value, std::get<i>(t));
    });
}

template<class T>
void func(std::size_t counter, simdjson::ondemand::value& value, T& ts) {
    func_impl(counter,
              value,
              ts,
              std::apply(
                  []<typename... Ts>(Ts...) {
                      return std::index_sequence_for<Ts...>{};
                  },
                  ts));
}
}// namespace detail

template<Concepts::SchemaBuffer SB>
struct JSONAdapter {
    constexpr static fnv1ah32 fnv1a{};
    constexpr static auto phfs = std::apply(
        []<typename... Fields>(Fields...) {
            return phf::make_phf({
                fnv1a(Fields::Name)...,
            });
        },
        typename SB::Schema::Fields{});

    static typename SB::Schema::TupleType parse(simdjson::ondemand::object& obj) {
        typename SB::Schema::TupleType result;
        for (auto field : obj) {
            auto key = field.unescaped_key().take_value();
            auto index = phfs.lookup(fnv1a(key));
            auto value = field.value().take_value();
            detail::func(index, value, result);
        }
        return result;
    }

    static std::string_view parseIntoBuffer(const simdjson::padded_string& data, NES::Runtime::TupleBuffer& buffer) {
        using namespace simdjson;
        ondemand::parser parser;
        ondemand::document_stream docs = parser.iterate_many(data);
        for (auto doc : docs) {
            ondemand::object obj;
            SIMDJSON_ERROR(doc.get_object().get(obj));
            auto t = parse(obj);
            SB(buffer).writeTuple(t);
        }
        return {};
    }

    static std::pair<std::string_view, typename SB::Schema::TupleType> parseAndAdvance(const simdjson::padded_string& data) {
        using namespace simdjson;
        ondemand::parser parser;
        ondemand::object obj;
        auto doc = parser.iterate(data);
        SIMDJSON_ERROR(doc.get_object().get(obj));
        auto t = parse(obj);
        std::string_view obj_view;
        SIMDJSON_ERROR(obj.raw_json().get(obj_view));
        return {std::string_view{obj_view.cend(), data.data() + data.length()}, t};
    }
};

}// namespace NES::DataParser::JSON
#endif// NES_DATA_PARSER_INCLUDE_GENERATION_JSONADAPTER_HPP_
