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

/**
 * @brief AddressSanitizer (ASan) detection and manual poisoning support.
 *
 * <sanitizer/asan_interface.h> ships with the compiler and already defines ASAN_POISON_MEMORY_REGION /
 * ASAN_UNPOISON_MEMORY_REGION as no-ops when ASan is off, so call sites need no #ifdefs. We only add
 * NES_ASAN_ENABLED for the places that must size a redzone at compile time.
 * See https://github.com/google/sanitizers/wiki/AddressSanitizerManualPoisoning.
 */
#include <sanitizer/asan_interface.h> /// NOLINT(misc-include-cleaner)

/// Nested, not `defined(__has_feature) && __has_feature(...)`: the preprocessor does not guarantee the
/// second operand is left unexpanded on compilers without __has_feature.
#if defined(__has_feature)
    #if __has_feature(address_sanitizer)
inline constexpr bool NES_ASAN_ENABLED = true;
    #elif defined(__SANITIZE_ADDRESS__)
inline constexpr bool NES_ASAN_ENABLED = true;
    #else
inline constexpr bool NES_ASAN_ENABLED = false;
    #endif
#elif defined(__SANITIZE_ADDRESS__)
inline constexpr bool NES_ASAN_ENABLED = true;
#else
inline constexpr bool NES_ASAN_ENABLED = false;
#endif
