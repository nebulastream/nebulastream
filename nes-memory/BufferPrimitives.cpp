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

#include <utility>
#include <Runtime/BufferPrimitives.hpp>

#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::Memory
{

namespace detail
{

template <bool isPinned>
RefCountedBCB<isPinned>::RefCountedBCB() : controlBlock(nullptr)
{
}
template <bool isPinned>
RefCountedBCB<isPinned>::RefCountedBCB(BufferControlBlock* controlBlock) : controlBlock(controlBlock)
{
    controlBlock->dataRetain();
    if constexpr (isPinned)
    {
        controlBlock->pinnedRetain();
    }
}
template <bool isPinned>
RefCountedBCB<isPinned>::RefCountedBCB(const RefCountedBCB& other) : controlBlock(other.controlBlock)
{
    if (controlBlock != nullptr)
    {
        controlBlock->dataRetain();
        if constexpr (isPinned)
        {
            controlBlock->pinnedRetain();
        }
    }
}
template <bool isPinned>
RefCountedBCB<isPinned>::RefCountedBCB(RefCountedBCB&& other) noexcept : controlBlock(other.controlBlock)
{
    other.controlBlock = nullptr;
}
template <bool isPinned>
RefCountedBCB<isPinned>& RefCountedBCB<isPinned>::operator=(const RefCountedBCB& other)
{
    if (this == &other)
    {
        return *this;
    }
    auto* oldControlBlock = std::exchange(controlBlock, other.controlBlock);

    /// Update reference counts: If the new and old controlBlocks differ, retain the new one and release the old one.
    if (oldControlBlock != controlBlock)
    {
        controlBlock->dataRetain();
        if constexpr (isPinned)
        {
            controlBlock->pinnedRetain();
        }
        if (oldControlBlock != nullptr)
        {
            oldControlBlock->dataRelease();
            if constexpr (isPinned)
            {
                oldControlBlock->pinnedRelease();
            }
        }
    }
    return *this;
}
template <bool isPinned>
RefCountedBCB<isPinned>& RefCountedBCB<isPinned>::operator=(RefCountedBCB&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    controlBlock = other.controlBlock;
    other.controlBlock = nullptr;
    return *this;
}
template <bool isPinned>
BufferControlBlock& RefCountedBCB<isPinned>::operator*() const noexcept
{
    return *controlBlock;
}
template <bool isPinned>
RefCountedBCB<isPinned>::~RefCountedBCB() noexcept
{
    if (controlBlock != nullptr)
    {
        controlBlock->dataRelease();
        if constexpr (isPinned)
        {
            controlBlock->pinnedRelease();
        }
    }
}

RepinBCBLock::RepinBCBLock(BufferControlBlock* controlBlock) noexcept : controlBlock(controlBlock)
{
    INVARIANT(controlBlock->isRepinning.test(), "Created repin lock on control block that is not getting repinned");
}

template class RefCountedBCB<true>;
template class RefCountedBCB<false>;

RepinBCBLock::RepinBCBLock(RepinBCBLock&& other) noexcept
{
    this->controlBlock = other.controlBlock;
    other.controlBlock = nullptr;
}
RepinBCBLock& RepinBCBLock::operator=(RepinBCBLock&& other) noexcept
{
    this->controlBlock = other.controlBlock;
    other.controlBlock = nullptr;
    return *this;
}
RepinBCBLock::~RepinBCBLock() noexcept
{
    // INVARIANT(lock.owns_lock(), "Unlocking repin lock on another thread");
    if (controlBlock)
    {
        controlBlock->markRepinningDone();
    }
}

bool RepinBCBLock::isOwner() const noexcept
{
    return controlBlock->isRepinning.test();
}
bool UniqueMutexBCBLock::isOwner() const noexcept
{
    return lock.owns_lock() && controlBlock->segmentMutex.native_handle() == lock.mutex()->native_handle();
}
bool SharedMutexBCBLock::isOwner() const noexcept
{
    return lock.owns_lock() && controlBlock->segmentMutex.native_handle() == lock.mutex()->native_handle();
}

}
}