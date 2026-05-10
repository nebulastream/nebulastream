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

#include <memory>
#include <utility>
#include <ErrorHandling.hpp>

namespace NES
{

/// Nullable, immutable, value-semantic holder that allows a type to (indirectly) contain itself, e.g., a DataType whose element type is
/// another DataType. `T` may be incomplete at the point where `Box<T>` is declared as a member.
/// Copies share the underlying storage, which is safe because the held value is const. Comparison is deep, so defaulted comparison
/// operators of the enclosing type keep value semantics. Similar to C++26's `std::indirect`, but nullable and cheap to copy.
template <typename T>
class Box
{
public:
    Box() = default;

    /// Implicit on purpose, so that a `T` can be passed wherever a `Box<T>` is expected.
    Box(T value) : ptr(std::make_shared<const T>(std::move(value))) { } /// NOLINT(google-explicit-constructor)

    [[nodiscard]] bool hasValue() const { return ptr != nullptr; }

    [[nodiscard]] const T& operator*() const
    {
        PRECONDITION(ptr != nullptr, "Cannot dereference an empty Box");
        return *ptr;
    }

    [[nodiscard]] const T* operator->() const { return &**this; }

    friend bool operator==(const Box& lhs, const Box& rhs)
    {
        if (lhs.ptr == rhs.ptr)
        {
            /// Same storage or both empty
            return true;
        }
        if (lhs.ptr == nullptr or rhs.ptr == nullptr)
        {
            return false;
        }
        return *lhs.ptr == *rhs.ptr;
    }

private:
    std::shared_ptr<const T> ptr;
};

}
