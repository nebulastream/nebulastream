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

#include <TaggedPointer.hpp>

#include <cstdint>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

static constexpr uintptr_t TAG_SHIFT = 48;

template <typename T>
TaggedPointer<T>::TaggedPointer(T* ptr, const uint16_t tag)
{
    reset(ptr, tag);
}

template <typename T>
TaggedPointer<T>& TaggedPointer<T>::operator=(T* ptr)
{
    reset(ptr);
    return *this;
}

template <typename T>
TaggedPointer<T>::operator bool() const
{
    return get() != nullptr;
}

template <typename T>
void TaggedPointer<T>::reset(T* ptr, const uint16_t tag)
{
    auto pointer = reinterpret_cast<uintptr_t>(ptr);
    PRECONDITION(!(pointer >> TAG_SHIFT), "tag out of range");
    pointer |= static_cast<uintptr_t>(tag) << TAG_SHIFT;
    data = pointer;
}

/// explicit instantiation of tagged ptr
template class TaggedPointer<Memory::detail::BufferControlBlock>;

}
