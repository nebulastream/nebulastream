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

#include <Configurations/ConfigField.hpp>

fmt::context::iterator fmt::formatter<NES::ConfigLiteral>::format(const NES::ConfigLiteral& literal, format_context& ctx) const
{
    return fmt::formatter<std::string_view>{}.format(
        std::visit(
            NES::Overloaded{
                [](const std::monostate&) { return std::string{"null"}; },
                [](const NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>& schema)
                {
                    std::ostringstream oss;
                    oss << schema;
                    return std::move(oss).str();
                },
                [](const auto& value) { return fmt::format("{}", value); }},
            literal),
        ctx);
}
