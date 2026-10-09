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

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace NES::Lookback
{

/// The row template of a LOOKBACK call, split once at lowering time so no parsing happens per
/// alert. `{}` marks a column slot, filled positionally from the lookback query's result columns —
/// the same convention as FMT, deliberately, so the two read alike inside one alert expression.
///
/// Represented as the literal runs *between* the placeholders: `a{}b{}c` becomes
/// {"a", "b", "c"} with two placeholders, and the invariant `literals.size() == placeholders + 1`
/// makes rendering a straight alternation with no bounds arithmetic.
class RowTemplate
{
public:
    /// @throws InvalidLiteral on an unmatched `{`, a stray `}`, or a non-empty placeholder body
    ///         such as `{Wert}` — substitution is positional, and silently ignoring a name the
    ///         author expected to be looked up would corrupt a clinical alert.
    static RowTemplate parse(std::string_view text);

    [[nodiscard]] size_t placeholderCount() const { return literals.empty() ? 0 : literals.size() - 1; }

    /// Length of the rendering of a row whose column values have the given total length.
    [[nodiscard]] size_t literalBytes() const { return literalByteCount; }

    /// Appends the rendering of one result row to `out`.
    /// @throws LookbackFailure if the row's column count does not match placeholderCount().
    void render(const std::vector<std::string>& row, std::string& out) const;

private:
    std::vector<std::string> literals;
    size_t literalByteCount = 0;
};

/// Counts the `?` parameter markers of an SQL statement, skipping any that sit inside a
/// single-quoted string literal (`''` is the SQL escape for an embedded quote). The ODBC source
/// counts markers with a plain scan; a lookback query is user-authored prose far more likely to
/// carry a literal containing `?`, so this one is quote-aware.
size_t countParameterMarkers(std::string_view sql);

}
