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
#include <Model/RunnableTestFile.hpp>
#include <Rewriter/SqlParse.hpp>

namespace NES
{

/// The catalog-legal key of one runnable test, which is used to prefix every name the test declares.
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
    /// A file that is not under the root is keyed by its absolute path, so a directly given file runs from anywhere.
    ///
    /// Example, with `/root/nes-systests` as the discovery root:
    ///     /root/nes-systests/benchmark/Nexmark.test           part 0 of 1 -> BENCHMARK_D_NEXMARK
    ///     /root/nes-systests/benchmark/Nexmark.test           part 1 of 3 -> BENCHMARK_D_NEXMARK_C1
    ///     /root/nes-systests/regression/2025-09-10_Join.test  part 0 of 1 -> REGRESSION_D_2025_2D_09_2D_10__JOIN
    ///     /elsewhere/Nexmark.test                             part 0 of 1 -> _D_ELSEWHERE_D_NEXMARK
    [[nodiscard]] TestFileKey keyOf(const std::filesystem::path& testFile, size_t part, size_t parts) const;

private:
    std::filesystem::path canonicalRoot;
};

/// Maps a canonical name to its prefixed spelling.
/// The key type compares and hashes by canonical form, so two unquoted spellings differing only in case share an entry.
using PrefixedByName = std::unordered_map<Identifier, Identifier>;

/// The catalog-visible names of one test file and each name's spelling, fixed once the registry has seen every name.
/// The rewriter takes this rather than the registry that built it, so it cannot rewrite a statement while names are still missing.
class PrefixedNames
{
public:
    /// Returns the prefixed spelling of a registered name, and nullopt for a name that is not registered.
    /// The rewriter substitutes catalog-visible names and leaves column names, aliases and keywords untouched.
    [[nodiscard]] std::optional<Identifier> prefixed(std::string_view name) const;

    /// The original spelling of every prefixed name, so a consumer can read a printed plan as the test wrote it.
    [[nodiscard]] OriginalNames originalNames() const;

private:
    friend class NameRegistry;
    explicit PrefixedNames(PrefixedByName prefixedByName);

    PrefixedByName prefixedByName;
};

/// Records which test file of one invocation declared each prefix.
/// The key encoding keeps two files apart, but the declared name follows the key as written,
/// so `d_b_s` in `a.test` and `s` in `a/b.test` both spell `A_D_B_S`.
class PrefixedNameOwners
{
public:
    void claim(const OriginalNames& names, const std::filesystem::path& testFile);

private:
    struct Owner
    {
        std::filesystem::path testFile;
        std::string originalName;
    };

    std::unordered_map<std::string, Owner> ownerBySpelling;
};

/// Collects the catalog-visible names of one test file, prefixing each with that file's key.
/// Every test file of one invocation can then share a single catalog.
class NameRegistry
{
public:
    explicit NameRegistry(const TestFileKey& testFileKey);

    /// Returns the prefixed spelling of a name, registering it on the first encounter.
    /// Two spellings with the same canonical form map to one prefixed name, so declaring a name twice is a no-op.
    /// Throws for a name that no identifier can hold, which the grammar admits between quotes.
    Identifier declare(std::string_view name);

    /// Hands the registered names to the rewriting that follows.
    /// Rvalue-qualified, so the call site shows that sealing consumes the registry.
    [[nodiscard]] PrefixedNames seal() &&;

private:
    std::string key;
    PrefixedByName prefixedByName;
};

/// Replaces every prefixed name in a text with the originally written text.
/// Only whole identifiers that are registered names change.
/// One pass over the text, so a restored name is never matched again: with key `ORDERS`, `ORDERS_ORDERS_INPUT` becomes
/// `ORDERS_INPUT` even when a source `input` is registered as well.
std::string restoreNames(std::string_view text, const OriginalNames& names);

/// Replaces every reference to a name that the test file declared with its prefixed spelling.
/// Only the grammar positions that hold a source, sink or model name change,
/// so a column/alias/plugin/function are untouched:
/// `SELECT s FROM s` renames only the second `s`.
void prefixNames(const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const PrefixedNames& names);

}
