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

template<typename T>
class TaggedPointer {
    static constexpr uintptr_t pointer_mask = 0x0000ffffffffffffull;
    static constexpr uintptr_t original_tag = 0x7fff000000000000ull;
    static constexpr uintptr_t tag_mask = ~pointer_mask;

  public:
    constexpr TaggedPointer() noexcept : raw(0) {}

    constexpr TaggedPointer(std::nullptr_t) noexcept : raw(0) {}

    constexpr TaggedPointer(uintptr_t value) noexcept : raw(value) {}

    explicit TaggedPointer(T* pointer, uint16_t tag = 0) noexcept {
        _address = extractPointer(reinterpret_cast<uintptr_t>(pointer));
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

    friend bool operator==(const TaggedPointer& lhs, const TaggedPointer& rhs) { return lhs.raw == rhs.raw; }

    friend bool operator!=(const TaggedPointer& lhs, const TaggedPointer& rhs) { return lhs.raw != rhs.raw; }

    friend bool operator==(const TaggedPointer& lhs, const std::nullptr_t&) { return lhs.raw == 0; }

    friend bool operator!=(const TaggedPointer& lhs, const std::nullptr_t&) { return lhs.raw != 0; }

    explicit operator bool() { return _address != 0; }

    explicit operator bool() const { return _address != 0; }

    T* operator->() const noexcept { return pointer(); }

    T& operator*() const noexcept { return *pointer(); }

    inline T* pointer() const { return reinterpret_cast<T*>(_address); }

    inline uint16_t tag() { return _tag; }

    explicit operator T*() { return pointer(); }

    explicit operator uintptr_t() { return raw; }

    inline uintptr_t internal() const { return raw; }

  private:
    static constexpr uintptr_t extractPointer(uintptr_t ptr) { return (ptr & pointer_mask); }

  private:
    union {
        struct {
            uintptr_t _address : 48;
            uintptr_t _tag : 16;
        };
        uintptr_t raw;
        T* _debug;
    };
};

#else
#error "unsupported platform -- you should implement generic tagged pointer"
#endif
}// namespace NES::Runtime
#pragma clang diagnostic pop
#endif//NES_NES_CORE_INCLUDE_RUNTIME_TAGGEDPOINTER_HPP_
