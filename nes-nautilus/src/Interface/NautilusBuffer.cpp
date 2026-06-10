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


#include <Interface/NautilusBuffer.hpp>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <variant>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_details.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>

namespace NES
{
nautilus::val<int8_t*> OwnedNautilusBuffer::data()
{
    return nautilus::invoke(+[](NES::TupleBuffer* buffer) { return buffer->getAvailableMemoryArea<int8_t>().data(); }, &buffer);
}

nautilus::val<const int8_t*> OwnedNautilusBuffer::data() const
{
    return nautilus::invoke(+[](const NES::TupleBuffer* buffer) { return buffer->getAvailableMemoryArea<int8_t>().data(); }, asArg());
}

nautilus::val<size_t> OwnedNautilusBuffer::getNumberOfRecords() const
{
    return nautilus::invoke(
        +[](const NES::TupleBuffer* buffer)
        {
            INVARIANT(buffer->getAvailableMemoryArea<>().data() != nullptr, "Buffer is invalid for NautilusBuffer:getNumberOfRecords()");
            return buffer->getNumberOfTuples();
        },
        asArg());
}

/// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): && signals ownership transfer; the nautilus tracer only needs the buffer's address.
nautilus::val<size_t> OwnedNautilusBuffer::storeChild(OwnedNautilusBuffer&& child)
{
    return nautilus::invoke(
        +[](NES::TupleBuffer* buffer, NES::TupleBuffer* child)
        { return static_cast<size_t>(buffer->storeChildBuffer(*child).getRawIndex()); },
        &buffer,
        &child.buffer);
}

/// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): && signals ownership transfer; the nautilus tracer only needs the buffer's address.
nautilus::val<size_t> OwnedNautilusBuffer::storeChild(BorrowedNautilusBuffer&& child)
{
    return nautilus::invoke(
        +[](NES::TupleBuffer* buffer, NES::TupleBuffer* child)
        { return static_cast<size_t>(buffer->storeChildBuffer(*child).getRawIndex()); },
        &buffer,
        child.asArg());
}

OwnedNautilusBuffer OwnedNautilusBuffer::getChild(const nautilus::val<size_t>& index) const
{
    OwnedNautilusBuffer child;
    nautilus::invoke(
        +[](const TupleBuffer* self, TupleBuffer* child, size_t index)
        { *child = self->loadChildBuffer(VariableSizedAccess::Index{index}); },
        asArg(),
        child.asArg(),
        index);
    return child;
}

nautilus::val<bool> OwnedNautilusBuffer::isValid() const
{
    return nautilus::invoke(+[](const NES::TupleBuffer* buffer) { return buffer->getAvailableMemoryArea<>().data() != nullptr; }, asArg());
}

nautilus::val<const NES::TupleBuffer*> OwnedNautilusBuffer::asArg() const&
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): nautilus::val has no const-qualified conversion; we widen back to const through the return type.
    return &const_cast<nautilus::val<NES::TupleBuffer>&>(buffer);
}

nautilus::val<NES::TupleBuffer*> OwnedNautilusBuffer::asArg() &
{
    return &buffer;
}

BorrowedNautilusBuffer::BorrowedNautilusBuffer(const nautilus::val<NES::TupleBuffer*>& buffer) : buffer(buffer)
{
}

BorrowedNautilusBuffer BorrowedNautilusBuffer::from(const nautilus::val<const TupleBuffer*>& originalBuffer)
{
    return BorrowedNautilusBuffer{originalBuffer};
}

nautilus::val<int8_t*> BorrowedNautilusBuffer::data()
{
    return nautilus::invoke(+[](NES::TupleBuffer* buffer) { return buffer->getAvailableMemoryArea<int8_t>().data(); }, buffer);
}

nautilus::val<const int8_t*> BorrowedNautilusBuffer::data() const
{
    return nautilus::invoke(+[](const NES::TupleBuffer* buffer) { return buffer->getAvailableMemoryArea<int8_t>().data(); }, buffer);
}

nautilus::val<size_t> BorrowedNautilusBuffer::getNumberOfRecords() const
{
    return nautilus::invoke(+[](const NES::TupleBuffer* buffer) { return buffer->getNumberOfTuples(); }, buffer);
}

OwnedNautilusBuffer BorrowedNautilusBuffer::getChild(const nautilus::val<size_t>& index) const
{
    OwnedNautilusBuffer child;
    nautilus::invoke(
        +[](TupleBuffer* self, TupleBuffer* child, size_t index) { *child = self->loadChildBuffer(VariableSizedAccess::Index{index}); },
        buffer,
        child.asArg(),
        index);
    return child;
}

/// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): && signals ownership transfer; the nautilus tracer only needs the buffer's address.
nautilus::val<size_t> BorrowedNautilusBuffer::storeChild(OwnedNautilusBuffer&& child)
{
    return nautilus::invoke(
        +[](TupleBuffer* self, TupleBuffer* child) { return self->storeChildBuffer(*child).getRawIndex(); }, buffer, child.asArg());
}

/// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): && signals ownership transfer; the nautilus tracer only needs the buffer's address.
nautilus::val<size_t> BorrowedNautilusBuffer::storeChild(BorrowedNautilusBuffer&& child)
{
    return nautilus::invoke(
        +[](TupleBuffer* self, TupleBuffer* child) { return self->storeChildBuffer(*child).getRawIndex(); }, buffer, child.asArg());
}

nautilus::val<NES::TupleBuffer*> BorrowedNautilusBuffer::asArg()
{
    return buffer;
}

nautilus::val<const int8_t*> NautilusBuffer::data() const
{
    return std::visit(Overloaded{[](const auto& underlying) { return underlying.data(); }}, underlying);
}

nautilus::val<int8_t*> NautilusBuffer::data()
{
    return std::visit(Overloaded{[](auto& underlying) { return underlying.data(); }}, underlying);
}

/// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): the moved-from state is transferred via buffer.underlying below.
nautilus::val<size_t> NautilusBuffer::storeChild(NautilusBuffer&& buffer)
{
    return std::visit(
        Overloaded{
            [](auto& underlying, auto&& other) -> nautilus::val<size_t>
            { return underlying.storeChild(std::forward<decltype(other)>(other)); }},
        underlying,
        std::move(buffer.underlying));
}

OwnedNautilusBuffer NautilusBuffer::getChild(const nautilus::val<size_t>& index) const
{
    return std::visit(Overloaded{[&](const auto& underlying) -> OwnedNautilusBuffer { return underlying.getChild(index); }}, underlying);
}

nautilus::val<const NES::TupleBuffer*> BorrowedNautilusBuffer::asArg() const
{
    return buffer;
}

NautilusBuffer::NautilusBuffer(OwnedNautilusBuffer buffer) : underlying(std::move(buffer))
{
}

NautilusBuffer::NautilusBuffer(BorrowedNautilusBuffer buffer) : underlying(std::move(buffer))
{
}

nautilus::val<size_t> NautilusBuffer::getNumberOfRecords() const
{
    return std::visit(Overloaded{[](const auto& underlying) { return underlying.getNumberOfRecords(); }}, underlying);
}

nautilus::val<const TupleBuffer*> NautilusBuffer::asArg() const&
{
    return std::visit(Overloaded{[](const auto& underlying) { return underlying.asArg(); }}, underlying);
}

nautilus::val<TupleBuffer*> NautilusBuffer::asArg() &
{
    return std::visit(Overloaded{[](auto& underlying) { return underlying.asArg(); }}, underlying);
}

bool NautilusBuffer::isOwned() const
{
    return std::holds_alternative<OwnedNautilusBuffer>(underlying);
}

OwnedNautilusBuffer OwnedNautilusBuffer::copy(const NautilusBuffer& source)
{
    OwnedNautilusBuffer owned;
    /// Copy-assigning the TupleBuffer takes a reference-counted copy of the source's underlying buffer, so the new owned buffer keeps
    /// it alive on its own. source.asArg() works uniformly for an owned or a borrowed source.
    nautilus::invoke(+[](TupleBuffer* self, const TupleBuffer* src) { *self = *src; }, owned.asArg(), source.asArg());
    return owned;
}
}
