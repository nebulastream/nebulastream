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

#ifndef NES_DATA_PARSER_INCLUDE_DATAPARSER_RUNTIMESCHEMAADAPTER_HPP_
#define NES_DATA_PARSER_INCLUDE_DATAPARSER_RUNTIMESCHEMAADAPTER_HPP_

#include <API/Schema.hpp>
#include <generation/SchemaBuffer.hpp>

template<NES::DataParser::Concepts::Schema S>
static NES::SchemaPtr runtimeSchema() {
    auto schema = NES::Schema::create();
    std::apply(
        [&schema]<NES::DataParser::Concepts::Field... Fs>(Fs...) {
            return ((schema = schema->addField(Fs::Name, Fs::Type::NESType)), ...);
        },
        typename S::Fields{});
    return schema;
}
#endif// NES_DATA_PARSER_INCLUDE_DATAPARSER_RUNTIMESCHEMAADAPTER_HPP_
