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

/// RE2 part ported from Codon's codon/runtime/re.cpp (Copyright Exaloop Inc., Apache 2.0) to the NES Codon runtime.
/// Differences: the pattern cache owns its keys and is process-wide (see `cache`), and patterns RE2 rejects fall back
/// to PCRE2, which supports Python's lookaround and backreferences at the cost of backtracking.

#include <RegexBackend.hpp>

#define PCRE2_CODE_UNIT_WIDTH 8
#include <re2/re2.h>
#include <pcre2.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

extern "C" void* seq_alloc_atomic(size_t size);

struct NesRegexPattern
{
    std::unique_ptr<re2::RE2> re2;
    /// Set only when RE2 rejected the pattern and PCRE2 accepted it.
    std::unique_ptr<pcre2_code, decltype(&pcre2_code_free)> pcre2{nullptr, &pcre2_code_free};
};

namespace
{
/// Flag bits of Codon's re module.
constexpr int64_t ignoreCase = 1 << 2;
constexpr int64_t multiLine = 1 << 4;
constexpr int64_t dotAll = 1 << 5;

/// Anchor values of Codon's re module, equal to re2::RE2::Anchor.
constexpr int64_t anchorStart = 1;
constexpr int64_t anchorBoth = 2;

re2::RE2::Options toRe2Options(const int64_t flags)
{
    re2::RE2::Options options;
    options.set_log_errors(false);
    options.set_encoding(re2::RE2::Options::Encoding::EncodingLatin1);
    options.set_case_sensitive((flags & ignoreCase) == 0);
    options.set_one_line((flags & multiLine) == 0);
    options.set_dot_nl((flags & dotAll) != 0);
    return options;
}

/// Byte mode without PCRE2_UTF, matching RE2's Latin-1 encoding above.
uint32_t toPcre2Options(const int64_t flags)
{
    uint32_t options = 0;
    options |= (flags & ignoreCase) != 0 ? PCRE2_CASELESS : 0;
    options |= (flags & multiLine) != 0 ? PCRE2_MULTILINE : 0;
    options |= (flags & dotAll) != 0 ? PCRE2_DOTALL : 0;
    return options;
}

std::string_view view(const SeqStr& s)
{
    return {s.str, static_cast<size_t>(s.len)};
}

SeqStr copyToUdf(const std::string_view value)
{
    auto* data = static_cast<char*>(seq_alloc_atomic(value.size()));
    std::memcpy(data, value.data(), value.size());
    return {static_cast<int64_t>(value.size()), data};
}

std::unique_ptr<NesRegexPattern> compile(const std::string& pattern, const int64_t flags)
{
    auto compiled = std::make_unique<NesRegexPattern>();
    compiled->re2 = std::make_unique<re2::RE2>(pattern, toRe2Options(flags));
    if (!compiled->re2->ok())
    {
        int errorCode = 0;
        PCRE2_SIZE errorOffset = 0;
        compiled->pcre2.reset(pcre2_compile(
            reinterpret_cast<PCRE2_SPTR>(pattern.data()), pattern.size(), toPcre2Options(flags), &errorCode, &errorOffset, nullptr));
    }
    return compiled;
}

/// Matches with PCRE2 and returns the spans of the first `groupCount` groups, all {-1, -1} without a match.
/// ponytail: hitting PCRE2's default match limit (10M backtracking steps) counts as no match, because Codon's re module
/// has no error channel for matching; add one to re.codon if a UDF needs to tell the two apart.
std::vector<SeqSpan>
pcre2Match(const pcre2_code* code, const int64_t anchor, const SeqStr s, const int64_t pos, const int64_t endpos, const uint32_t groupCount)
{
    std::vector<SeqSpan> spans(groupCount, SeqSpan{-1, -1});
    const std::unique_ptr<pcre2_match_data, decltype(&pcre2_match_data_free)> matchData{
        pcre2_match_data_create_from_pattern(code, nullptr), &pcre2_match_data_free};
    uint32_t options = 0;
    options |= anchor == anchorStart || anchor == anchorBoth ? PCRE2_ANCHORED : 0;
    options |= anchor == anchorBoth ? PCRE2_ENDANCHORED : 0;
    /// Python's endpos truncates the subject, so it is passed as the subject length.
    const auto result = pcre2_match(code, reinterpret_cast<PCRE2_SPTR>(s.str), endpos, pos, options, matchData.get(), nullptr);
    if (result < 0)
    {
        return spans;
    }
    const auto* ovector = pcre2_get_ovector_pointer(matchData.get());
    const auto setGroups = std::min(groupCount, pcre2_get_ovector_count(matchData.get()));
    for (uint32_t i = 0; i < setGroups; ++i)
    {
        if (ovector[2 * i] != PCRE2_UNSET)
        {
            spans[i] = {static_cast<int64_t>(ovector[2 * i]), static_cast<int64_t>(ovector[(2 * i) + 1])};
        }
    }
    return spans;
}

uint32_t pcre2GroupCount(const pcre2_code* code)
{
    uint32_t count = 0;
    pcre2_pattern_info(code, PCRE2_INFO_CAPTURECOUNT, &count);
    return count;
}

/// Codon's cache is thread_local and keys on the caller's pattern memory. In NES that memory is a per-invocation arena,
/// and a pattern compiled by the module initializer on one worker thread is used by all of them, so the cache copies
/// its keys and lives for the whole process.
/// ponytail: never evicted, bounded by the distinct patterns the UDFs use.
std::mutex cacheMutex;
std::map<std::pair<std::string, int64_t>, std::unique_ptr<NesRegexPattern>, std::less<>> cache;
}

extern "C" SeqSpan* seq_re_match(NesRegexPattern* pattern, const int64_t anchor, const SeqStr s, const int64_t pos, const int64_t endpos)
{
    const auto groupCount = seq_re_pattern_groups(pattern) + 1;
    auto* spans = static_cast<SeqSpan*>(seq_alloc_atomic(groupCount * sizeof(SeqSpan)));
    if (pattern->pcre2)
    {
        const auto matched = pcre2Match(pattern->pcre2.get(), anchor, s, pos, endpos, static_cast<uint32_t>(groupCount));
        std::memcpy(spans, matched.data(), groupCount * sizeof(SeqSpan));
        return spans;
    }

    std::vector<std::string_view> groups(groupCount);
    if (!pattern->re2->Match(view(s), pos, endpos, static_cast<re2::RE2::Anchor>(anchor), groups.data(), static_cast<int>(groupCount)))
    {
        groups.assign(groupCount, {});
    }
    for (int64_t i = 0; i < groupCount; ++i)
    {
        spans[i] = groups[i].data() == nullptr
            ? SeqSpan{-1, -1}
            : SeqSpan{groups[i].data() - s.str, groups[i].data() - s.str + static_cast<int64_t>(groups[i].size())};
    }
    return spans;
}

extern "C" SeqSpan seq_re_match_one(NesRegexPattern* pattern, const int64_t anchor, const SeqStr s, const int64_t pos, const int64_t endpos)
{
    if (pattern->pcre2)
    {
        return pcre2Match(pattern->pcre2.get(), anchor, s, pos, endpos, 1)[0];
    }
    std::string_view match;
    if (!pattern->re2->Match(view(s), pos, endpos, static_cast<re2::RE2::Anchor>(anchor), &match, 1))
    {
        return {-1, -1};
    }
    return {match.data() - s.str, match.data() - s.str + static_cast<int64_t>(match.size())};
}

extern "C" SeqStr seq_re_escape(const SeqStr pattern)
{
    return copyToUdf(re2::RE2::QuoteMeta(view(pattern)));
}

extern "C" NesRegexPattern* seq_re_compile(const SeqStr pattern, const int64_t flags)
{
    const std::scoped_lock lock(cacheMutex);
    auto key = std::make_pair(std::string{view(pattern)}, flags);
    auto& entry = cache[key];
    if (!entry)
    {
        entry = compile(key.first, flags);
    }
    return entry.get();
}

extern "C" void seq_re_purge()
{
    /// Compiled patterns may still be referenced by module globals of other UDFs, so purging is a no-op.
}

extern "C" int64_t seq_re_pattern_groups(NesRegexPattern* pattern)
{
    return pattern->pcre2 ? pcre2GroupCount(pattern->pcre2.get()) : pattern->re2->NumberOfCapturingGroups();
}

extern "C" int64_t seq_re_group_name_to_index(NesRegexPattern* pattern, const SeqStr name)
{
    const std::string groupName{view(name)};
    if (pattern->pcre2)
    {
        const auto index = pcre2_substring_number_from_name(pattern->pcre2.get(), reinterpret_cast<PCRE2_SPTR>(groupName.c_str()));
        return index < 0 ? -1 : index;
    }
    const auto& mapping = pattern->re2->NamedCapturingGroups();
    const auto it = mapping.find(groupName);
    return it != mapping.end() ? it->second : -1;
}

extern "C" SeqStr seq_re_group_index_to_name(NesRegexPattern* pattern, const int64_t index)
{
    if (pattern->pcre2)
    {
        /// Each name table entry is a big-endian 16-bit group number followed by the NUL-terminated name.
        uint32_t nameCount = 0;
        uint32_t entrySize = 0;
        PCRE2_SPTR table = nullptr;
        pcre2_pattern_info(pattern->pcre2.get(), PCRE2_INFO_NAMECOUNT, &nameCount);
        pcre2_pattern_info(pattern->pcre2.get(), PCRE2_INFO_NAMEENTRYSIZE, &entrySize);
        pcre2_pattern_info(pattern->pcre2.get(), PCRE2_INFO_NAMETABLE, &table);
        for (uint32_t i = 0; i < nameCount; ++i)
        {
            const auto* entry = table + (static_cast<size_t>(i) * entrySize);
            if (((entry[0] << 8) | entry[1]) == index)
            {
                return copyToUdf(reinterpret_cast<const char*>(entry + 2));
            }
        }
        return {0, nullptr};
    }
    const auto& mapping = pattern->re2->CapturingGroupNames();
    const auto it = mapping.find(static_cast<int>(index));
    return it != mapping.end() ? copyToUdf(it->second) : SeqStr{0, nullptr};
}

extern "C" bool seq_re_check_rewrite_string(NesRegexPattern* pattern, const SeqStr rewrite, SeqStr* error)
{
    /// RE2-specific and unused by re.codon, which expands replacement templates itself.
    if (pattern->pcre2)
    {
        return true;
    }
    std::string message;
    const bool valid = pattern->re2->CheckRewriteString(view(rewrite), &message);
    if (!valid)
    {
        *error = copyToUdf(message);
    }
    return valid;
}

extern "C" SeqStr seq_re_pattern_error(NesRegexPattern* pattern)
{
    /// When PCRE2 rejects the pattern too, RE2's message is reported.
    return pattern->re2->ok() || pattern->pcre2 ? SeqStr{0, nullptr} : copyToUdf(pattern->re2->error());
}
