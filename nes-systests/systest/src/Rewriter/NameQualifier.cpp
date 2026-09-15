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
#include <unordered_set>
#include <utility>

#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>
#include <fmt/format.h>

#include <Identifiers/Identifier.hpp>
#include <Rewriter/SqlParse.hpp>
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

TestFileKey::TestFileKey(std::string key) : key{std::move(key)}
{
}

/// `weakly_canonical` returns a normalized absolute path: /repo/./systests becomes /repo/systests.
DiscoveryRoot::DiscoveryRoot(const std::filesystem::path& root) : canonicalRoot{std::filesystem::weakly_canonical(root)}
{
}

TestFileKey DiscoveryRoot::keyOf(const std::filesystem::path& testFile, const size_t part, const size_t parts) const
{
    PRECONDITION(part < parts, "part {} of test file {} does not exist, the file has {} parts", part, testFile.string(), parts);
    /// Normalize both paths before subtraction.
    /// Relating an absolute path to a relative one gives an empty result instead of an error, leading to collisions.
    /// The command line decides the discovery root (which may be absolute or relative), while a discovered test file is always absolute.
    const auto canonicalPath = std::filesystem::weakly_canonical(testFile);
    /// Pure path arithmetic, does not interact with the filesystem.
    auto relative = canonicalPath.lexically_relative(canonicalRoot);
    /// A file above or unrelated to the root has no position under the root and therefore no key.
    if (relative.empty() or *relative.begin() == std::filesystem::path{".."})
    {
        throw TestException("test file {} is not located under the discovery root {}", testFile.string(), canonicalRoot.string());
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
    /// A file with a single part keeps its own key, and every further part gets a suffix of its own.
    /// No encoded path ends in a bare part suffix, because a lone underscore only occurs inside a token,
    /// so a file named like another file's part cannot take that part's key.
    return TestFileKey{parts == 1 ? key : fmt::format("{}_C{}", key, part)};
}

std::string unqualified(const std::string_view text, const std::string_view qualifyingPrefix)
{
    return replaceAll(text, qualifyingPrefix, "");
}

NameRegistry::NameRegistry(const TestFileKey& testFileKey) : key{testFileKey.value()}
{
}

Identifier NameRegistry::declare(const std::string_view name)
{
    /// The grammar admits any text between quotes, so a test file can declare a name that no identifier can hold.
    /// Such a file is malformed, so we throw.
    auto parsed = Identifier::tryParse(std::string{name});
    if (not parsed)
    {
        throw TestException("declared name {} is not a legal identifier: {}", name, parsed.error().what());
    }
    auto identifier = std::move(*parsed);
    if (const auto existing = qualifiedByName.find(identifier); existing != qualifiedByName.end())
    {
        return existing->second;
    }

    /// A quoted name keeps case/punctuation declared in the test, so its qualified version stays quoted.
    /// This conditional requotes the combined key if it originally was quoted.
    /// With the key `KEY`:
    ///     bid            -> KEY_BID
    ///     "INPUT STREAM" -> "KEY_INPUT STREAM"
    /// Without quotes, the second gives KEY_INPUT STREAM, which is two tokens and does not parse.
    auto spelling = identifier.isCaseSensitive() ? fmt::format(R"("{}_{}")", key, identifier.asCanonicalString())
                                                 : fmt::format("{}_{}", key, identifier.asCanonicalString());
    /// A key holds no dot and starts with no digit, and a canonical name holds neither a dot nor a quote,
    /// so the spelling is a legal identifier and parsing it back cannot fail.
    auto qualified = Identifier::parse(std::move(spelling));
    /// The key is a fixed prefix and the canonical name is new, so the result has to be unique.
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

std::optional<Identifier> QualifiedNames::qualified(const std::string_view name) const
{
    /// A token that no identifier can hold was never registered.
    const auto parsed = Identifier::tryParse(std::string{name});
    if (not parsed)
    {
        return std::nullopt;
    }
    if (const auto found = qualifiedByName.find(*parsed); found != qualifiedByName.end())
    {
        return found->second;
    }
    return std::nullopt;
}

void qualifyNames(SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const QualifiedNames& names)
{
    /// The grammar interprets a plugin type as a plain identifier, so only the parse tree can distinguish them.
    std::unordered_set<size_t> typeTokens;
    const auto keepType = [&typeTokens](const AntlrSQLParser::IdentifierContext* type)
    {
        if (type != nullptr)
        {
            typeTokens.insert(type->getStart()->getTokenIndex());
        }
    };
    for (const auto* source : findAll<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parse.tree()))
    {
        keepType(source->type);
    }
    for (const auto* sink : findAll<AntlrSQLParser::CreateSinkDefinitionContext>(parse.tree()))
    {
        keepType(sink->type);
    }
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parse.tree()))
    {
        keepType(source->type);
    }
    for (const auto* sink : findAll<AntlrSQLParser::AnonymousSinkContext>(parse.tree()))
    {
        keepType(sink->type);
    }

    for (auto* token : parse.tokenStream().getTokens())
    {
        if (const auto type = token->getType(); type != AntlrSQLLexer::IDENTIFIER and type != AntlrSQLLexer::BACKQUOTED_IDENTIFIER)
        {
            continue;
        }
        if (typeTokens.contains(token->getTokenIndex()))
        {
            continue;
        }
        if (const auto qualified = names.qualified(token->getText()))
        {
            rewriter.replace(token, std::string{qualified->getOriginalString()});
        }
    }
}

}
