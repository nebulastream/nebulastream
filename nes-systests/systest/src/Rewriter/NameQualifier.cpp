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

#include <Rewriter/NameQualifier.hpp>

#include <cctype>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <fmt/format.h>

#include <Identifiers/Identifier.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// One character of a test file key.
/// A letter or a digit passes through, and everything else becomes a token between underscores: `__` an underscore,
/// `_D_` a directory separator, and `_<hex>_` any other byte.
/// A token cannot be mistaken for passed-through text, so decoding is unambiguous.
std::string encodeKeyCharacter(const char character)
{
    if (std::isalnum(static_cast<unsigned char>(character)) != 0)
    {
        return std::string{character};
    }
    if (character == '_')
    {
        return "__";
    }
    if (character == '/')
    {
        return "_D_";
    }
    return fmt::format("_{:02X}_", static_cast<unsigned char>(character));
}

}

std::string getTestFileKey(const std::filesystem::path& testFile, const std::filesystem::path& discoveryRoot)
{
    /// Both paths take the same normal form before one is subtracted from the other.
    /// Relating an absolute path to a relative one gives an empty result rather than an error, and every file of the run would then
    /// key the same and collide in the shared catalog.
    /// The command line decides the discovery root, while a discovered test file is always absolute.
    const auto canonicalRoot = std::filesystem::weakly_canonical(discoveryRoot);
    auto relative = std::filesystem::weakly_canonical(testFile).lexically_relative(canonicalRoot);
    /// A file above the root, or unrelated to it, has no position under the root and therefore no key.
    if (relative.empty() or *relative.begin() == std::filesystem::path{".."})
    {
        throw TestException("test file {} is not located under the discovery root {}", testFile.string(), discoveryRoot.string());
    }
    relative.replace_extension();

    /// The relative path without its extension is the raw key, so the directory keeps files sharing a stem apart.
    /// The per-character encoding is reversible, so no two paths share a key.
    /// Two paths differing only in case share a key, because an unquoted identifier folds case anyway.
    const auto folded = toUpperCase(relative.generic_string());
    auto key = folded | std::views::transform(encodeKeyCharacter) | std::views::join | std::ranges::to<std::string>();
    /// An unquoted identifier may not start with a digit, so a leading digit is encoded like a special character.
    /// No path encodes to a leading token on its own, because a relative path does not start with a separator.
    if (std::isdigit(static_cast<unsigned char>(key.front())) != 0)
    {
        key = fmt::format("_{:02X}_{}", static_cast<unsigned char>(key.front()), key.substr(1));
    }
    return key;
}

std::string getTestFilePartKey(const std::string& testFileKey, const size_t part, const size_t parts)
{
    return parts == 1 ? testFileKey : fmt::format("{}_C{}", testFileKey, part);
}

std::string unqualified(const std::string_view text, const std::string_view qualifyingPrefix)
{
    return replaceAll(text, qualifyingPrefix, "");
}

NameRegistry::NameRegistry(std::string testFileKey) : key{std::move(testFileKey)}
{
}

std::string NameRegistry::declare(const std::string_view name)
{
    auto identifier = Identifier::parse(std::string{name});
    if (const auto existing = qualifiedByName.find(identifier); existing != qualifiedByName.end())
    {
        return existing->second;
    }

    /// A quoted name keeps the case and punctuation the test wrote, and only quotes preserve that through the catalog.
    /// An unquoted `A_INPUT STREAM` for a source declared as `"INPUT STREAM"` produces a statement that does not parse.
    auto qualified = identifier.isCaseSensitive() ? fmt::format(R"("{}_{}")", key, identifier.asCanonicalString())
                                                  : fmt::format("{}_{}", key, identifier.asCanonicalString());
    /// A spelling already taken means two distinct canonical names collided, which the prefix cannot represent reversibly.
    /// Rejecting it is better than overwriting the earlier name.
    INVARIANT(!takenSpellings.contains(qualified), "two names in this test file qualify to the same spelling '{}'", qualified);

    takenSpellings.emplace(qualified);
    qualifiedByName.emplace(std::move(identifier), qualified);
    return qualified;
}

QualifiedNames NameRegistry::seal() &&
{
    return QualifiedNames{std::move(qualifiedByName), fmt::format("{}_", key)};
}

QualifiedNames::QualifiedNames(QualifiedByName qualifiedByName, std::string prefix)
    : qualifiedByName{std::move(qualifiedByName)}, prefix{std::move(prefix)}
{
}

std::optional<std::string> QualifiedNames::qualified(const std::string_view name) const
{
    if (const auto found = qualifiedByName.find(Identifier::parse(std::string{name})); found != qualifiedByName.end())
    {
        return found->second;
    }
    return std::nullopt;
}

std::string rewriteIdentifiers(const std::string_view sql, const QualifiedNames& names)
{
    antlr4::ANTLRInputStream input{sql.data(), sql.length()};
    AntlrSQLLexer lexer{&input};

    /// Copies the original text and replaces each registered identifier token with its qualified spelling.
    /// Token positions are character offsets, so untouched runs including whitespace and comments are copied rather than relexed.
    std::string rewritten;
    size_t cursor = 0;
    for (const auto& token : lexer.getAllTokens())
    {
        if (const auto type = token->getType(); type == AntlrSQLLexer::IDENTIFIER || type == AntlrSQLLexer::BACKQUOTED_IDENTIFIER)
        {
            if (const auto qualified = names.qualified(token->getText()))
            {
                rewritten.append(sql.substr(cursor, token->getStartIndex() - cursor));
                rewritten.append(*qualified);
                cursor = token->getStopIndex() + 1;
            }
        }
    }
    rewritten.append(sql.substr(cursor));
    return rewritten;
}

}
