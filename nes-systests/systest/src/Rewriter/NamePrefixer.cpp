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

#include <Rewriter/NamePrefixer.hpp>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <AntlrSQLParser.h>
#include <ParserRuleContext.h>
#include <TokenStreamRewriter.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Identifiers/Identifier.hpp>
#include <Model/RunnableTestFile.hpp>
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

/// The root is stored absolute, because subtracting a relative root from an absolute file gives an empty result,
/// which would key every file of the run by its absolute path.
/// `absolute` resolves against the current directory without touching the filesystem, and `weakly_canonical`
/// normalizes the result: /repo/./systests becomes /repo/systests.
DiscoveryRoot::DiscoveryRoot(const std::filesystem::path& root)
    : canonicalRoot{std::filesystem::weakly_canonical(std::filesystem::absolute(root))}
{
}

TestFileKey DiscoveryRoot::keyOf(const std::filesystem::path& testFile, const size_t part, const size_t parts) const
{
    PRECONDITION(part < parts, "part {} of test file {} does not exist, the file has {} parts", part, testFile.string(), parts);
    /// Normalize both paths before subtraction.
    /// Relating an absolute path to a relative one gives an empty result, leading to collisions.
    /// The cli decides the discovery root (which may be absolute or relative), while a discovered test file is always absolute.
    const auto canonicalPath = std::filesystem::weakly_canonical(testFile);
    auto keyPath = canonicalPath.lexically_relative(canonicalRoot);
    /// A file above or unrelated to the root has no position under the root, so its absolute path is the key.
    /// Subtracting the root leaves only `..` and name components, never a leading separator,
    /// and only a separator encodes to `_D_`, so the keys of an outside and an inside file cannot collide.
    if (keyPath.empty() or *keyPath.begin() == std::filesystem::path{".."})
    {
        keyPath = canonicalPath;
    }
    keyPath.replace_extension();

    /// The path without its extension is the raw key, so the directory keeps files sharing a stem apart.
    /// The per-character encoding is reversible, so no two paths share a key.
    /// Two paths differing only in case share a key, because an unquoted identifier folds case anyway.
    const auto folded = toUpperCase(keyPath.generic_string());
    auto key = folded | std::views::transform(encodeKeyCharacter) | std::views::join | std::ranges::to<std::string>();
    /// An unquoted identifier may not start with a digit, so a leading digit is encoded like a special character.
    if (std::isdigit(static_cast<unsigned char>(key.front())) != 0)
    {
        key = fmt::format("_{:02X}_{}", static_cast<unsigned char>(key.front()), key.substr(1));
    }
    /// A file with a single part keeps its own key, and every further part gets a suffix of its own.
    /// No encoded path ends in a bare part suffix, because a lone underscore only occurs inside a token,
    /// so a file named like another file's part cannot take that part's key.
    return TestFileKey{parts == 1 ? key : fmt::format("{}_C{}", key, part)};
}

std::string restoreNames(const std::string_view text, const OriginalNames& names)
{
    if (names.empty())
    {
        return std::string{text};
    }
    auto prefixed = names | std::views::keys | std::ranges::to<std::vector<std::string>>();
    /// The only moves happen inside the library's sort, where the analyzer loses track and reports a moved-from string at the comparison.
    /// NOLINTNEXTLINE(clang-analyzer-cplusplus.Move)
    std::ranges::sort(prefixed, [](const auto& left, const auto& right) { return left.size() > right.size(); });
    const auto escape = [](const std::string& name)
    {
        static const std::regex Special{R"([.^$|()\[\]{}*+?\\])"};
        return std::regex_replace(name, Special, R"(\$&)");
    };
    const std::regex pattern{fmt::format(R"(\b(?:{})(?![A-Za-z0-9_]))", fmt::join(prefixed | std::views::transform(escape), "|"))};

    std::string restored;
    const std::string input{text};
    auto rest = input.cbegin();
    for (std::smatch match; std::regex_search(rest, input.cend(), match, pattern); rest = match[0].second)
    {
        restored.append(rest, match[0].first);
        restored.append(names.at(match[0].str()));
    }
    restored.append(rest, input.cend());
    return restored;
}

void PrefixedNameOwners::claim(const OriginalNames& names, const std::filesystem::path& testFile)
{
    for (const auto& [spelling, originalName] : names)
    {
        if (const auto [owner, inserted] = ownerBySpelling.try_emplace(spelling, Owner{.testFile = testFile, .originalName = originalName});
            not inserted)
        {
            throw TestException(
                "test files {} and {} both declare a name spelled {} after prefixing: {} and {}",
                owner->second.testFile.string(),
                testFile.string(),
                spelling,
                owner->second.originalName,
                originalName);
        }
    }
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
    if (const auto existing = prefixedByName.find(identifier); existing != prefixedByName.end())
    {
        return existing->second;
    }

    /// A quoted name keeps the case and punctuation declared in the test, so its prefixed version stays quoted.
    /// With the key `KEY`:
    ///     bid            -> KEY_BID
    ///     "INPUT STREAM" -> "KEY_INPUT STREAM"
    /// Without quotes, the second gives KEY_INPUT STREAM, which is two tokens and does not parse.
    auto spelling = identifier.isCaseSensitive() ? fmt::format(R"("{}_{}")", key, identifier.asCanonicalString())
                                                 : fmt::format("{}_{}", key, identifier.asCanonicalString());
    /// A key holds no dot and starts with no digit, and a canonical name holds neither a dot nor a quote,
    /// so the spelling is a legal identifier and parsing it back cannot fail.
    auto prefixed = Identifier::parse(std::move(spelling));
    /// The key is a fixed prefix and the canonical name is new, so the result has to be unique.
    prefixedByName.emplace(std::move(identifier), prefixed);
    return prefixed;
}

PrefixedNames NameRegistry::seal() &&
{
    return PrefixedNames{std::move(prefixedByName)};
}

PrefixedNames::PrefixedNames(PrefixedByName prefixedByName) : prefixedByName{std::move(prefixedByName)}
{
}

OriginalNames PrefixedNames::originalNames() const
{
    OriginalNames originals;
    for (const auto& [original, prefixed] : prefixedByName)
    {
        originals.emplace(prefixed.asCanonicalString(), original.asCanonicalString());
    }
    return originals;
}

std::optional<Identifier> PrefixedNames::prefixed(const std::string_view name) const
{
    /// A token that no identifier can hold was never registered.
    const auto parsed = Identifier::tryParse(std::string{name});
    if (not parsed)
    {
        return std::nullopt;
    }
    if (const auto found = prefixedByName.find(*parsed); found != prefixedByName.end())
    {
        return found->second;
    }
    return std::nullopt;
}

void prefixNames(const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const PrefixedNames& names)
{
    const auto prefix = [&](antlr4::ParserRuleContext* identifier)
    {
        if (identifier == nullptr)
        {
            return;
        }
        if (const auto prefixed = names.prefixed(identifier->getText()))
        {
            rewriter.replace(identifier->getStart(), std::string{prefixed->getOriginalString()});
        }
    };
    /// A declared name has no dot, so a reference is a single part.
    const auto prefixParts = [&](const AntlrSQLParser::MultipartIdentifierContext* multipart)
    {
        for (auto* part : multipart->parts)
        {
            prefix(part->identifier());
        }
    };

    for (const auto* source : findAll<AntlrSQLParser::CreateLogicalSourceDefinitionContext>(parse.tree()))
    {
        prefix(source->sourceName);
    }
    for (const auto* source : findAll<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parse.tree()))
    {
        prefix(source->logicalSource);
    }
    for (const auto* sink : findAll<AntlrSQLParser::CreateSinkDefinitionContext>(parse.tree()))
    {
        prefix(sink->sinkName);
    }
    for (const auto* model : findAll<AntlrSQLParser::CreateModelDefinitionContext>(parse.tree()))
    {
        prefix(model->modelName);
    }
    for (auto* source : findAll<AntlrSQLParser::NamedSourceContext>(parse.tree()))
    {
        prefixParts(source->multipartIdentifier());
    }
    for (const auto* inference : findAll<AntlrSQLParser::ModelInferenceSourceContext>(parse.tree()))
    {
        prefix(inference->modelName);
    }
    for (auto* input : findAll<AntlrSQLParser::ModelInferenceStreamNameContext>(parse.tree()))
    {
        prefixParts(input->multipartIdentifier());
    }
    for (auto* sink : findAll<AntlrSQLParser::SinkContext>(parse.tree()))
    {
        prefix(sink->identifier());
    }
    /// A qualified field reference spells the source before the dot, and the parser checks that spelling against the source.
    for (const auto* dereference : findAll<AntlrSQLParser::DereferenceContext>(parse.tree()))
    {
        if (auto* qualifier = dynamic_cast<AntlrSQLParser::ColumnReferenceContext*>(dereference->base))
        {
            prefix(qualifier->identifier());
        }
    }
    for (auto* star : findAll<AntlrSQLParser::StarContext>(parse.tree()))
    {
        if (auto* qualified = star->qualifiedName(); qualified != nullptr)
        {
            prefix(qualified->identifier(0));
        }
    }
}

}
