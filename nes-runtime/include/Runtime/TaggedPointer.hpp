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

#ifndef NES_NES_RUNTIME_INCLUDE_RUNTIME_TAGGEDPOINTER_HPP_
#define NES_NES_RUNTIME_INCLUDE_RUNTIME_TAGGEDPOINTER_HPP_

#include <Util/Logger/Logger.hpp>
#include <folly/Likely.h>
#include <folly/Portability.h>

#if !(defined(__x86_64__) || defined(_M_X64)) && !(defined(__arm__)) && !(defined(__powerpc64__))
#error "TaggedPointer is x64, arm64 and ppc64 specific code."
#endif



namespace NES {
template<typename T>
class TaggedPointer {
  public:
    explicit TaggedPointer(T* ptr, uint16_t tag = 0) { reset(ptr, tag); }

    TaggedPointer& operator=(T* ptr) {
        reset(ptr);
        return *this;
    }

    explicit operator bool() const { return get() != nullptr; }

    inline bool operator!() { return get() == nullptr; }

    inline T* get() { return static_cast<T*>(pointer()); }

    inline const T* get() const { return static_cast<const T*>(pointer()); }

    inline const T& operator*() const { return *get(); }

    inline T& operator*() { return *get(); }

    inline const T* operator->() const { return get(); }

    inline T* operator->() { return get(); }

    inline uint16_t tag() const { return data >> 48; }

    inline void* pointer() const { return reinterpret_cast<void*>(data & ((1ULL << 48) - 1)); }

    inline void reset(T* ptr = nullptr, uint16_t tag = 0) {
        uintptr_t pointer = reinterpret_cast<uintptr_t>(ptr);
        NES_ASSERT(!(pointer >> 48), "invalid pointer");
        pointer |= static_cast<uintptr_t>(tag) << 48;
        data = pointer;
    }

  private:
    uintptr_t data = 0;
};

}// namespace NES

#endif//NES_NES_RUNTIME_INCLUDE_RUNTIME_TAGGEDPOINTER_HPP_
