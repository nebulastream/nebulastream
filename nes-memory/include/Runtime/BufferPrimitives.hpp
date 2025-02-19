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
#include <optional>

namespace NES::Memory
{

namespace detail
{
class ChildOrMainDataKey;
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
    constexpr ChildOrMainDataKey(ChildKey& childKey) noexcept;
    constexpr ChildOrMainDataKey(const ChildOrMainDataKey& other) = default;
    [[nodiscard]] constexpr inline std::optional<ChildKey> asChildKey() const;

    constexpr size_t getNum() const noexcept;
    constexpr ChildOrMainDataKey(ChildOrMainDataKey&& other) noexcept;

    constexpr ChildOrMainDataKey& operator=(const ChildOrMainDataKey& other) noexcept;
    constexpr ChildOrMainDataKey& operator=(ChildOrMainDataKey&& other) noexcept;


    constexpr friend bool operator==(const ChildOrMainDataKey& lhs, const ChildOrMainDataKey& rhs);
    constexpr friend bool operator!=(const ChildOrMainDataKey& lhs, const ChildOrMainDataKey& rhs);
    static constexpr ChildOrMainDataKey UNKNOWN() noexcept;
    static constexpr ChildOrMainDataKey MAIN() noexcept;
};
constexpr ChildOrMainDataKey ChildOrMainDataKey::UNKNOWN() noexcept
{
    return ChildOrMainDataKey(0);
}
constexpr ChildOrMainDataKey ChildOrMainDataKey::MAIN() noexcept
{
    return ChildOrMainDataKey(1);
}

constexpr ChildOrMainDataKey& ChildOrMainDataKey::operator=(const ChildOrMainDataKey& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    num = other.num;
    return *this;
}
constexpr ChildOrMainDataKey& ChildOrMainDataKey::operator=(ChildOrMainDataKey&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    num = other.num;
    other.num = 0;
    return *this;
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
    if (num > 1)
    {
        return ChildKey{num - 2};
    }
    return std::nullopt;
}
constexpr size_t ChildOrMainDataKey::getNum() const noexcept
{
    return num;
}
constexpr ChildOrMainDataKey::ChildOrMainDataKey(ChildOrMainDataKey&& other) noexcept : num(other.num)
{
    other.num = 0;
}
constexpr ChildOrMainDataKey::ChildOrMainDataKey(ChildKey& childKey) noexcept : num(childKey + 2)
{
}

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
}

constexpr ChildKey::operator detail::ChildOrMainDataKey() const
{
    return detail::ChildOrMainDataKey{num + 2};
}
}