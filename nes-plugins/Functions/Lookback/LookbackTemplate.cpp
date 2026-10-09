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

#include <LookbackTemplate.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <ErrorHandling.hpp>

namespace NES::Lookback
{

RowTemplate RowTemplate::parse(std::string_view text)
{
    RowTemplate result;
    std::string current;
    for (size_t i = 0; i < text.size(); ++i)
    {
        const auto character = text[i];
        if (character == '}')
        {
            throw InvalidLiteral("LOOKBACK row template has a '}}' without a matching '{{' at offset {}: '{}'", i, text);
        }
        if (character != '{')
        {
            current.push_back(character);
            continue;
        }

        const auto closing = text.find('}', i);
        if (closing == std::string_view::npos)
        {
            throw InvalidLiteral("LOOKBACK row template has an unmatched '{{' at offset {}: '{}'", i, text);
        }
        if (closing != i + 1)
        {
            throw InvalidLiteral(
                "LOOKBACK row template placeholder at offset {} is '{}', but substitution is positional — use an empty "
                "'{{}}' for each result column, in column order: '{}'",
                i,
                text.substr(i, closing - i + 1),
                text);
        }

        result.literalByteCount += current.size();
        result.literals.emplace_back(std::move(current));
        current.clear();
        i = closing;
    }
    result.literalByteCount += current.size();
    result.literals.emplace_back(std::move(current));
    return result;
}

void RowTemplate::render(const std::vector<std::string>& row, std::string& out) const
{
    if (row.size() != placeholderCount())
    {
        throw LookbackFailure(
            "LOOKBACK row template has {} placeholder(s) but the lookback query returned {} column(s)", placeholderCount(), row.size());
    }

    out.append(literals.front());
    for (size_t i = 0; i < row.size(); ++i)
    {
        out.append(row[i]);
        out.append(literals[i + 1]);
    }
}

size_t countParameterMarkers(std::string_view sql)
{
    size_t count = 0;
    bool inStringLiteral = false;
    for (size_t i = 0; i < sql.size(); ++i)
    {
        const auto character = sql[i];
        if (character == '\'')
        {
            /// `''` inside a literal is an escaped quote, not a terminator: skip both characters so
            /// the literal stays open.
            if (inStringLiteral && i + 1 < sql.size() && sql[i + 1] == '\'')
            {
                ++i;
                continue;
            }
            inStringLiteral = !inStringLiteral;
            continue;
        }
        if (character == '?' && !inStringLiteral)
        {
            ++count;
        }
    }
    return count;
}

}
