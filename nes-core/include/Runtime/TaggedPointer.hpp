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
#ifndef NES_NES_CORE_INCLUDE_RUNTIME_TAGGEDPOINTER_HPP_
#define NES_NES_CORE_INCLUDE_RUNTIME_TAGGEDPOINTER_HPP_

#include <cstddef>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-anonymous-struct"
#pragma clang diagnostic ignored "-Wnested-anon-types"

namespace NES::Runtime {

#if (defined(__x86_64__) || defined(_M_X64) || defined(__s390x__))

// on x86 pointers starts with the same 16 bits

/**
 * @brief An immutable pointer holder with a tag
 * @tparam T the internal object type to store
 */
template<typename T>
class TaggedPointer {
    static constexpr uintptr_t POINTER_MASK = 0x0000ffffffffffffull;
    static constexpr uintptr_t ORIGINAL_TAG = 0x7fff000000000000ull;
    static constexpr uintptr_t TAG_MASK = ~POINTER_MASK;

  public:
    /// @brief initialize empty tagged pointer
    constexpr TaggedPointer() noexcept : raw(0) {}

    /// @brief initialize empty tagged pointer
    constexpr TaggedPointer(std::nullptr_t) noexcept : raw(0) {}

    /// @brief initialize using value
    constexpr TaggedPointer(uintptr_t value) noexcept : raw(value) {}

    /// @brief initialize using pointer and tag
    explicit TaggedPointer(T* pointer, uint16_t tag = 0) noexcept {
        auto ptr = reinterpret_cast<uintptr_t>(pointer);
        _address = ptr & POINTER_MASK;
        _tag = tag;
    }

    TaggedPointer(const TaggedPointer& that) noexcept : raw(that.raw) {}

    TaggedPointer(TaggedPointer&& that) noexcept : raw(that.raw) { that.raw = 0; }

    TaggedPointer& operator=(const TaggedPointer& that) {
        raw = that.raw;
        return *this;
    }

    TaggedPointer& operator=(TaggedPointer&& that) noexcept {
        if (this == std::addressof(that)) {
            return *this;
        }
        using std::swap;
        swap(*this, that);

        return *this;
    }

    TaggedPointer& operator=(std::nullptr_t) noexcept {
        raw = 0;
        return *this;
    }

    inline friend void swap(TaggedPointer& lhs, TaggedPointer& rhs) noexcept {
        using std::swap;

        swap(lhs.raw, rhs.raw);
    }

    friend bool operator==(const TaggedPointer& lhs, const TaggedPointer& rhs) noexcept { return lhs.raw == rhs.raw; }

    friend bool operator!=(const TaggedPointer& lhs, const TaggedPointer& rhs) noexcept { return lhs.raw != rhs.raw; }

    friend bool operator==(const TaggedPointer& lhs, const std::nullptr_t&) noexcept { return lhs.raw == 0; }

    friend bool operator!=(const TaggedPointer& lhs, const std::nullptr_t&) noexcept { return lhs.raw != 0; }

    operator bool() noexcept { return _address != 0; }

    operator bool() const noexcept { return _address != 0; }

    T* operator->() const noexcept { return pointer(); }

    T& operator*() const noexcept { return *pointer(); }

    /**
     * @brief Provides the internal address as pointer (no tag)
     * @return the internal pointer without its tag
     */
    inline T* pointer() const noexcept { return reinterpret_cast<T*>(_address); }

    /**
     * @brief Provides the tag of the pointer
     * @return the tag of the pointer
     */
    inline uint16_t tag() const noexcept { return _tag; }

    explicit operator T*() noexcept { return pointer(); }

    explicit operator uintptr_t() noexcept { return raw; }

    /**
     * @brief Provides the internal address+tag as an unsigned long
     * @return the internal pointer+tag as an unsigned long
     */
    inline uintptr_t internal() const noexcept { return raw; }

  private:
    union {
        struct {
            uintptr_t _address : 48;
            uintptr_t _tag : 16;
        };
        uintptr_t raw;
        T* debug;
    };
};

#else
#error "unsupported platform -- you should implement generic tagged pointer"
#endif
}// namespace NES::Runtime
#pragma clang diagnostic pop
#endif//NES_NES_CORE_INCLUDE_RUNTIME_TAGGEDPOINTER_HPP_
