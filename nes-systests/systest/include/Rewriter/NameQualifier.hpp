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

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Identifiers/Identifier.hpp>

namespace NES
{

/// Derives a catalog-legal key for a test file from its location under the discovery root.
/// The directory is part of the key, so two files sharing a stem in different directories get distinct keys.
/// Anything that is not legal in an unquoted identifier becomes an underscore.
std::string getTestFileKey(const std::filesystem::path& testFile, const std::filesystem::path& discoveryRoot);

/// Derives the qualification key of one part of a test file: the prefix on every catalog-visible name that the part declares.
/// A file that needs only one worker keeps its own key, so the SQL that it emits does not change.
/// A file split across workers needs one key per part, because one logical source declared on two workers under one name
/// would read from both instead of from the worker running the query.
std::string getTestFilePartKey(const std::string& testFileKey, size_t part, size_t parts);

/// Maps a canonical name to its qualified spelling.
/// The key type compares and hashes by canonical form, so two unquoted spellings differing only in case share an entry.
using QualifiedByName = std::unordered_map<Identifier, std::string>;
/// Maps a qualified spelling back to its original spelling.
using OriginalByQualified = std::unordered_map<std::string, std::string>;

/// The catalog-visible names of one test file and each name's spelling, fixed once the registry has seen every name.
/// The rewriter takes this rather than the registry that built it, so it cannot rewrite a statement while names are still missing.
class QualifiedNames
{
public:
    /// Returns the qualified spelling of a registered name, and nullopt for any other name.
    /// The rewriter substitutes catalog-visible names and leaves column names and aliases untouched.
    [[nodiscard]] std::optional<std::string> qualified(std::string_view name) const;

    /// Returns the original spelling of a qualified name, or nullopt when this never qualified it.
    /// Failure output prints the name that the test author wrote.
    [[nodiscard]] std::optional<std::string> original(std::string_view qualified) const;

private:
    friend class NameRegistry;
    QualifiedNames(QualifiedByName qualifiedByName, OriginalByQualified originalByQualified);

    QualifiedByName qualifiedByName;
    OriginalByQualified originalByQualified;
};

/// Collects the catalog-visible names of one test file, prefixing each with that file's key.
/// Every test file of one invocation can then share a single catalog.
/// Sealing hands the finished names to the rewriter and consumes the registry,
/// so no caller can add a name once the rewriting phase started.
class NameRegistry
{
public:
    /// Builds a registry for a test file, deriving its key from the file's location under the discovery root.
    static NameRegistry forTestFile(const std::filesystem::path& testFile, const std::filesystem::path& discoveryRoot);

    /// Takes a key that was already derived.
    /// Prefer the factory unless the key is in hand.
    explicit NameRegistry(std::string testFileKey);

    /// Returns the qualified spelling of a name, registering it on the first encounter.
    /// Two spellings with the same canonical form map to one qualified name, so declaring a name twice changes nothing.
    std::string declare(std::string_view name);

    /// Hands the registered names to the rewriting that follows.
    /// Rvalue-qualified, so the call site shows that the registry is consumed.
    [[nodiscard]] QualifiedNames seal() &&;

private:
    std::string key;
    QualifiedByName qualifiedByName;
    OriginalByQualified originalByQualified;
};

/// Removes the qualifying prefix wherever it occurs in a text, so a plan reads as the test wrote it.
/// Qualifying prefixes a name with the test file's key and an underscore, and an expected plan holds what the test wrote.
/// A plain text scan rather than a lookup, because a printed plan is not a statement and has no tokens to read.
std::string unqualified(std::string_view text, std::string_view qualifyingPrefix);

/// Rewrites the identifiers of a SQL statement to their qualified spellings and copies every other token unchanged.
/// Only registered names change, so column names, aliases, keywords and string literals stay as the test wrote them.
std::string rewriteIdentifiers(std::string_view sql, const QualifiedNames& names);

}
