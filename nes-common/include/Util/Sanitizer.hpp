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
 * Defines NES_ASAN_ENABLED (1 when built with ASan, 0 otherwise) and guarantees that the manual
 * poisoning macros ASAN_POISON_MEMORY_REGION / ASAN_UNPOISON_MEMORY_REGION are always available:
 * they call into the ASan runtime when ASan is enabled and expand to no-ops otherwise. This lets
 * callers poison/unpoison memory regions unconditionally without sprinkling #ifdefs at each call
 * site. See https://github.com/google/sanitizers/wiki/AddressSanitizerManualPoisoning.
 */
#if defined(__has_feature)
    #if __has_feature(address_sanitizer)
        #define NES_ASAN_ENABLED 1
    #endif
#endif
#if !defined(NES_ASAN_ENABLED) && defined(__SANITIZE_ADDRESS__)
    #define NES_ASAN_ENABLED 1
#endif
#if defined(NES_ASAN_ENABLED)
    #include <sanitizer/asan_interface.h>
#else
    #define NES_ASAN_ENABLED 0
    #ifndef ASAN_POISON_MEMORY_REGION
        #define ASAN_POISON_MEMORY_REGION(addr, size) (static_cast<void>(addr), static_cast<void>(size))
        #define ASAN_UNPOISON_MEMORY_REGION(addr, size) (static_cast<void>(addr), static_cast<void>(size))
    #endif
#endif
