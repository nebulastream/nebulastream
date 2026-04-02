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

/// Tagged inline macro for selective Nautilus function inlining.
///
/// Usage: Place NAUTILUS_TAGGED_INLINE(tag) in front of a function definition:
///
///     NAUTILUS_TAGGED_INLINE(parse_null)
///     ParseResult<bool>* parseFixedSized(const int8_t* addr, uint64_t size, ...) { ... }
///
/// Whether the function actually gets inlined is controlled by the CMake configuration:
///   - NES_INLINE_ALL=ON:   all tagged functions are inlined (global override)
///   - NES_INLINE_ALL=OFF:  only tags listed in NES_INLINE_TAGS are inlined
///
/// Tags not listed in the config cause a compile error, ensuring every tagged function
/// has been explicitly considered. To disable a tag, add it to NES_DISABLED_INLINE_TAGS.
///
/// After changing the CMake inline config, reconfigure and rebuild.

#pragma once
#include <nautilus/inline.hpp>
#include <InlineTagConfig.generated.hpp>

/// Helpers for two-level token pasting
#define NES_INLINE_CONCAT_(a, b) a##b
#define NES_INLINE_CONCAT(a, b) NES_INLINE_CONCAT_(a, b)

/// Map 1 → NAUTILUS_INLINE, 0 → nothing
#define NES_TAGGED_INLINE_EXPAND_1 NAUTILUS_INLINE
#define NES_TAGGED_INLINE_EXPAND_0

#if NES_INLINE_GLOBAL
/// Global inlining enabled: all tagged functions get NAUTILUS_INLINE regardless of per-tag settings
#define NAUTILUS_TAGGED_INLINE(tag) NAUTILUS_INLINE
#else
/// Per-tag inlining: NES_INLINE_TAG_<tag> must be defined as 0 or 1 in the generated config.
/// Undefined tags produce a compile error — add the tag to the CMake inline config.
#define NAUTILUS_TAGGED_INLINE(tag) NES_INLINE_CONCAT(NES_TAGGED_INLINE_EXPAND_, NES_INLINE_TAG_##tag)
#endif
