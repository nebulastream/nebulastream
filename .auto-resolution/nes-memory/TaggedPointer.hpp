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
#if !(defined(__x86_64__) || defined(_M_X64)) && !(defined(__powerpc64__)) && !(defined(__aarch64__))
    #error "TaggedPointer is x64, arm64 and ppc64 specific code."
#endif

namespace NES
{
/**
 * @brief This class represents a mutable pointer that stores a tag in the 16 msb bits
 * @tparam T
 */
template <typename T>
class TaggedPointer
{
public:
    /**
     * @brief Creates a TaggedPointer from a pointer and a tag
     * @param ptr
     * @param tag
     */
    explicit TaggedPointer(T* ptr, uint16_t tag = 0);

    /**
     * @brief Assign operators from a pointer of type T
     * @param ptr the new pointer
     * @return self
     */
    TaggedPointer& operator=(T* ptr);

    /**
     * @brief Bool converted, true if pointer is valid
     * @return
     */
    explicit operator bool() const;

    /**
     * @brief negate operator, true if pointer is not valid
     * @return
     */
    inline bool operator!() { return get() == nullptr; }

    /**
     * @brief returns the de-tagged pointer casted to T*
     * @return
     */
    inline T* get() { return static_cast<T*>(pointer()); }

    /**
     * @brief returns the de-tagged pointer casted to const T*
     * @return
     */
    inline const T* get() const { return static_cast<const T*>(pointer()); }

    /**
     * @brief returns the de-tagged pointer casted to T&
     * @return
     */
    inline const T& operator*() const { return *get(); }

    /**
     * @brief returns the de-tagged pointer casted to const T&
     * @return
     */
    inline T& operator*() { return *get(); }

    /**
     * @brief returns the de-tagged pointer casted to const T*
     * @return
     */
    inline const T* operator->() const { return get(); }

    /**
     * @brief returns the de-tagged pointer casted to T*
     * @return
     */
    inline T* operator->() { return get(); }

    /**
     * @brief returns the companion tag
     * @return
     */
    inline uint16_t tag() const { return data >> 48; }

    /**
     * @brief returns the de-tagged pointer casted to void*
     * @return
     */
    inline void* pointer() const { return reinterpret_cast<void*>(data & ((1ULL << 48) - 1)); }

    /**
     * @brief reset by mutating the internal pointer and the tag
     * @return
     */
    void reset(T* ptr = nullptr, uint16_t tag = 0);

private:
    uintptr_t data = 0;
};

}
