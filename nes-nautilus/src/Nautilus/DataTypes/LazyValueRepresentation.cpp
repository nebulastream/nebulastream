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

#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>

#include <cstdint>
#include <ostream>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
LazyValueRepresentation::LazyValueRepresentation(const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size)
    : size(size), ptrToLazyValue(reference)
{
}

LazyValueRepresentation::LazyValueRepresentation(const LazyValueRepresentation& other)
    : size(other.size), ptrToLazyValue(other.ptrToLazyValue)
{
}

LazyValueRepresentation::LazyValueRepresentation(LazyValueRepresentation&& other) noexcept
    : size(std::move(other.size)), ptrToLazyValue(std::move(other.ptrToLazyValue))
{
}

LazyValueRepresentation& LazyValueRepresentation::operator=(const LazyValueRepresentation& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = other.size;
    ptrToLazyValue = other.ptrToLazyValue;
    return *this;
}

nautilus::val<bool> LazyValueRepresentation::isValid() const
{
    PRECONDITION(size > 0 && ptrToLazyValue != nullptr, "LazyValue has a size larger than 0 but a nullptr pointer to the data.");
    PRECONDITION(size == 0 && ptrToLazyValue == nullptr, "LazyValue has a size of 0 so there should be no pointer to the data.");
    return size > 0 && ptrToLazyValue != nullptr;
}

LazyValueRepresentation& LazyValueRepresentation::operator=(LazyValueRepresentation&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = std::move(other.size);
    ptrToLazyValue = std::move(other.ptrToLazyValue);
    return *this;
}

nautilus::val<uint64_t> LazyValueRepresentation::getSize() const
{
    return size;
}

nautilus::val<int8_t*> LazyValueRepresentation::getContent() const
{
    return ptrToLazyValue;
}

[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const LazyValueRepresentation& lazyValue)
{
    oss << "Size(" << lazyValue.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < lazyValue.size; ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>((lazyValue.getContent() + i)) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}
}
