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

#include <cstdint>

/// The native half of Codon's re module (codon/re.codon). Codon implements these in its runtime library
/// (codon/runtime/re.cpp), which NES does not link, so this plugin provides them with the same ABI.
/// A pattern is matched by RE2, or by PCRE2 when RE2 rejects it (lookaround, backreferences).
struct NesRegexPattern;

struct SeqStr
{
    int64_t len;
    char* str;
};

struct SeqSpan
{
    int64_t start;
    int64_t end;
};

extern "C" SeqSpan* seq_re_match(NesRegexPattern* pattern, int64_t anchor, SeqStr s, int64_t pos, int64_t endpos);
extern "C" SeqSpan seq_re_match_one(NesRegexPattern* pattern, int64_t anchor, SeqStr s, int64_t pos, int64_t endpos);
extern "C" SeqStr seq_re_escape(SeqStr pattern);
extern "C" NesRegexPattern* seq_re_compile(SeqStr pattern, int64_t flags);
extern "C" void seq_re_purge();
extern "C" int64_t seq_re_pattern_groups(NesRegexPattern* pattern);
extern "C" int64_t seq_re_group_name_to_index(NesRegexPattern* pattern, SeqStr name);
extern "C" SeqStr seq_re_group_index_to_name(NesRegexPattern* pattern, int64_t index);
extern "C" bool seq_re_check_rewrite_string(NesRegexPattern* pattern, SeqStr rewrite, SeqStr* error);
extern "C" SeqStr seq_re_pattern_error(NesRegexPattern* pattern);
