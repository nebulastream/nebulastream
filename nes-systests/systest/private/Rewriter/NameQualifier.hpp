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
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <TokenStreamRewriter.h>

#include <Identifiers/Identifier.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// The catalog-legal key of one runnable test, which is used to qualify every name the test declares.
/// Only the discovery root constructs one, so a key holding an arbitrary string cannot reach a registry.
class TestFileKey
{
public:
    [[nodiscard]] const std::string& value() const { return key; }

private:
    friend class DiscoveryRoot;
    explicit TestFileKey(std::string key);

    std::string key;
};

/// The directory that every test file of one run is keyed relative to, in canonical form.
/// The `keyOf` member function is the only legal way (compiler-enforced) from a test file path to a `TestFileKey`.
class DiscoveryRoot
{
public:
    explicit DiscoveryRoot(const std::filesystem::path& root);

    /// Derives the key of one part of a test file from the file's location under the root.
    /// The directory is part of the key, so two files sharing a stem in different directories get distinct keys.
    /// The derivation is injective, so no two different files of one run can share a key and collide.
    /// A test file yields one runnable test per config combination, so each part needs a unique key.
    /// A file with a single part keeps its own key, so the SQL that it emits does not change.
    /// A file that is not under the root has no position under it and therefore no key, so we throw.
    ///
    /// Example, with `/root/nes-systests` as the discovery root:
    ///     /root/nes-systests/benchmark/Nexmark.test           part 0 of 1 -> BENCHMARK_D_NEXMARK
    ///     /root/nes-systests/benchmark/Nexmark.test           part 1 of 3 -> BENCHMARK_D_NEXMARK_C1
    ///     /root/nes-systests/regression/2025-09-10_Join.test  part 0 of 1 -> REGRESSION_D_2025_2D_09_2D_10__JOIN
    [[nodiscard]] TestFileKey keyOf(const std::filesystem::path& testFile, size_t part, size_t parts) const;

private:
    std::filesystem::path canonicalRoot;
};

/// Maps a canonical name to its qualified spelling.
/// The key type compares and hashes by canonical form, so two unquoted spellings differing only in case share an entry.
using QualifiedByName = std::unordered_map<Identifier, Identifier>;

/// The catalog-visible names of one test file and each name's spelling, fixed once the registry has seen every name.
/// The rewriter takes this rather than the registry that built it, so it cannot rewrite a statement while names are still missing.
class QualifiedNames
{
public:
    /// Returns the qualified spelling of a registered name, and nullopt for a name that is not registered.
    /// The rewriter substitutes catalog-visible names and leaves column names, aliases and keywords untouched.
    [[nodiscard]] std::optional<Identifier> qualified(std::string_view name) const;

    /// The prefix qualifying puts in front of every name, so a consumer can strip it from a text again.
    [[nodiscard]] const std::string& qualifyingPrefix() const { return prefix; }

private:
    friend class NameRegistry;
    QualifiedNames(QualifiedByName qualifiedByName, std::string prefix);

    QualifiedByName qualifiedByName;
    std::string prefix;
};

/// Collects the catalog-visible names of one test file, prefixing each with that file's key.
/// Every test file of one invocation can then share a single catalog.
class NameRegistry
{
public:
    explicit NameRegistry(const TestFileKey& testFileKey);

    /// Returns the qualified spelling of a name, registering it on the first encounter.
    /// Two spellings with the same canonical form map to one qualified name, so declaring a name twice is a no-op.
    /// Throws for a name that no identifier can hold, which the grammar admits between quotes.
    Identifier declare(std::string_view name);

    /// Hands the registered names to the rewriting that follows.
    /// Rvalue-qualified, so the call site shows that sealing consumes the registry.
    [[nodiscard]] QualifiedNames seal() &&;

private:
    std::string key;
    QualifiedByName qualifiedByName;
};

/// Removes the qualifying prefix wherever it occurs in text, so a plan reads as the test wrote it.
std::string unqualified(std::string_view text, std::string_view qualifyingPrefix);

/// Replaces every identifier that names something the test file declared with its qualified spelling.
/// Only registered names change, so column names, aliases, keywords and string literals stay as the test wrote them.
/// An identifier naming a plugin type keeps its spelling, because a type is not a name that any test file declares, and the
/// grammar admits the same word in both positions: `CREATE PHYSICAL SOURCE FOR File TYPE File` names a source and a plugin.
/// The replacements join the given rewriter, so a caller combines them with its own edits in one pass, and an edit that
/// already covers a name wins over the replacement of that name.
void qualifyNames(SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const QualifiedNames& names);

}
