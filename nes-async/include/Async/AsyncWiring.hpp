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

#pragma once

#include <string>
#include <string_view>
#include <unordered_map>

#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// Descriptors accept only the configuration keys they declare, and their values are
/// plain strings. An asynchronous operator, however, needs to hand its consumer source
/// two things of arbitrary shape: the schema of the records the producer sends, and the
/// settings of whichever executor it uses.
///
/// Both are therefore packed into a single declared parameter each, as JSON. JSON because
/// the values are arbitrary text — a prompt may contain any character, including whatever
/// separator an ad-hoc encoding would pick.
///
/// These functions are the only place that format is defined; the split rule writes it and
/// the consumer source reads it back.
std::string encodeConfig(const std::unordered_map<std::string, std::string>& config);
std::unordered_map<std::string, std::string> decodeConfig(std::string_view encoded);

std::string encodeSchema(const Schema<UnqualifiedUnboundField, Ordered>& schema);
Schema<UnqualifiedUnboundField, Ordered> decodeSchema(std::string_view encoded);

}
