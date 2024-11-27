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
#include <type_traits>
#include <Runtime/BufferPrimitives.hpp>
#include <Runtime/DataSegment.hpp>
#include <gtest/gtest_prod.h>

namespace NES::Memory
{
class PinnedBuffer;
namespace detail
{
class BufferControlBlock;
}

class FloatingBuffer
{
    FRIEND_TEST(BufferManagerTest, RepinUnpinContention);
    friend class PinnedBuffer;
    friend class BufferManager;

    detail::DataSegment<detail::DataLocation> getSegment() const noexcept;
public:

    [[nodiscard]] FloatingBuffer() noexcept;
    [[nodiscard]] FloatingBuffer(FloatingBuffer const& other) noexcept;

    /// @brief Move constructor: Steal the resources from `other`. This does not affect the reference count.
    /// @dev In this constructor, `other` is cleared, because otherwise its destructor would release its old memory.
    [[nodiscard]] FloatingBuffer(FloatingBuffer&& other) noexcept;

    [[nodiscard]] explicit FloatingBuffer(PinnedBuffer&& buffer) noexcept;

    FloatingBuffer& operator=(FloatingBuffer const& other) noexcept;

    FloatingBuffer& operator=(FloatingBuffer&& other) noexcept;

    friend void swap(FloatingBuffer& lhs, FloatingBuffer& rhs) noexcept;

    [[nodiscard]] bool isSpilled() const;
    ~FloatingBuffer() noexcept;

private:
    detail::BufferControlBlock* controlBlock;
    ///If == 0, then this floating buffer refers to the main data segment stored in the BCB.
    ///If == 1, then its unknown what this refer to, please search throught the children vector in the BCB.
    ///If > 1, then childOrMainData - 2 is the index of the child buffer in the BCB that this floating buffer belongs to.
    detail::ChildOrMainDataKey childOrMainData;
};

static_assert(std::is_copy_constructible_v<FloatingBuffer>);
static_assert(std::is_assignable_v<FloatingBuffer, FloatingBuffer>);

}