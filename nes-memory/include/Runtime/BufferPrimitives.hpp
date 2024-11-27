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
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>

namespace NES::Memory
{

namespace detail
{
class ChildOrMainDataKey;
class BufferControlBlock;
class ReturnToThread;

}
class ChildKey
{
    size_t num;

public:
    constexpr ChildKey(const size_t num) noexcept : num(num) { }
    constexpr ChildKey(const ChildKey& other) = default;
    constexpr ChildKey& operator=(const ChildKey& other) = default;
    constexpr operator detail::ChildOrMainDataKey() const;


    constexpr operator size_t() const { return num; }
    [[nodiscard]] constexpr size_t getNum() const { return num; }
};

namespace detail
{

class ChildOrMainDataKey
{
private:
    friend ChildKey;
    size_t num;
    constexpr explicit ChildOrMainDataKey(size_t num) noexcept : num(num) { }

public:
    constexpr ChildOrMainDataKey() noexcept : num(0) { }
    constexpr ChildOrMainDataKey(ChildKey& childKey) noexcept;
    constexpr ChildOrMainDataKey(const ChildOrMainDataKey& other) = default;
    [[nodiscard]] constexpr inline std::optional<ChildKey> asChildKey() const;

    constexpr size_t getNum() const noexcept;
    constexpr ChildOrMainDataKey(ChildOrMainDataKey&& other) noexcept = default;

    constexpr ChildOrMainDataKey& operator=(const ChildOrMainDataKey& other) noexcept = default;
    constexpr ChildOrMainDataKey& operator=(ChildOrMainDataKey&& other) noexcept = default;


    constexpr friend bool operator==(const ChildOrMainDataKey& lhs, const ChildOrMainDataKey& rhs);
    constexpr friend bool operator!=(const ChildOrMainDataKey& lhs, const ChildOrMainDataKey& rhs);
    static constexpr ChildOrMainDataKey UNKNOWN() noexcept;
    static constexpr ChildOrMainDataKey MAIN() noexcept;
};

static_assert(std::is_trivially_copyable_v<ChildOrMainDataKey>);
constexpr ChildOrMainDataKey ChildOrMainDataKey::UNKNOWN() noexcept
{
    return ChildOrMainDataKey(0);
}
constexpr ChildOrMainDataKey ChildOrMainDataKey::MAIN() noexcept
{
    return ChildOrMainDataKey(1);
}

constexpr bool operator==(const ChildOrMainDataKey& lhs, const ChildOrMainDataKey& rhs)
{
    return lhs.num == rhs.num;
}
constexpr bool operator!=(const ChildOrMainDataKey& lhs, const ChildOrMainDataKey& rhs)
{
    return !(lhs == rhs);
}

constexpr std::optional<ChildKey> ChildOrMainDataKey::asChildKey() const
{
    //make copy at current point to avoid data races
    //Does NOT mean that childkeys are atomic, just that this conversion uses the value from one point in time
    if (const auto currentNum = num; currentNum > 1)
    {
        return ChildKey{currentNum - 2};
    }
    return std::nullopt;
}
constexpr size_t ChildOrMainDataKey::getNum() const noexcept
{
    return num;
}
constexpr ChildOrMainDataKey::ChildOrMainDataKey(ChildKey& childKey) noexcept : num(childKey + 2)
{
}

static_assert(std::is_trivially_copyable_v<ChildOrMainDataKey>);

class BufferControlBlock;
template <bool isPinned>
class RefCountedBCB
{
    BufferControlBlock* controlBlock;

public:
    explicit RefCountedBCB();
    explicit RefCountedBCB(BufferControlBlock* controlBlock);
    RefCountedBCB(const RefCountedBCB& other);
    RefCountedBCB(RefCountedBCB&& other) noexcept;
    RefCountedBCB& operator=(const RefCountedBCB& other);
    RefCountedBCB& operator=(RefCountedBCB&& other) noexcept;

    BufferControlBlock& operator*() const noexcept;

    ~RefCountedBCB() noexcept;
};

class RepinBCBLock
{
    std::unique_lock<std::shared_mutex> lock;
    BufferControlBlock* controlBlock;

public:
    explicit RepinBCBLock(std::unique_lock<std::shared_mutex>&& lock, BufferControlBlock* control_block)
        : lock(std::move(lock)), controlBlock(control_block)
    {
    }
    RepinBCBLock(const RepinBCBLock& other) = delete;
    RepinBCBLock(RepinBCBLock&& other) noexcept = default;
    RepinBCBLock& operator=(const RepinBCBLock& other) = delete;
    RepinBCBLock& operator=(RepinBCBLock&& other) noexcept = default;
    ~RepinBCBLock() noexcept;


    operator std::unique_lock<std::shared_mutex>&()
    {
        return lock;
    }
};
class SpillBCBLock
{
    std::unique_lock<std::shared_mutex> lock;
    BufferControlBlock* controlBlock;

public:
    explicit SpillBCBLock(std::unique_lock<std::shared_mutex>&& lock, BufferControlBlock* control_block)
        : lock(std::move(lock)), controlBlock(control_block)
    {
    }
    SpillBCBLock(const SpillBCBLock& other) = delete;
    SpillBCBLock(SpillBCBLock&& other) noexcept = default;
    SpillBCBLock& operator=(const SpillBCBLock& other) = delete;
    SpillBCBLock& operator=(SpillBCBLock&& other) noexcept = default;
    ~SpillBCBLock() noexcept = default;


    operator std::unique_lock<std::shared_mutex>&()
    {
        return lock;
    }
};
}

constexpr ChildKey::operator detail::ChildOrMainDataKey() const
{
    return detail::ChildOrMainDataKey{num + 2};
}

}