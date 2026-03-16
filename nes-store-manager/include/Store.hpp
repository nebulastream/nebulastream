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

#include <concepts>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <nameof.hpp>

namespace NES::StoreManager
{

namespace detail
{
struct ErasedStore;
}

template <typename Checked = detail::ErasedStore>
struct TypedStore;
using Store = TypedStore<>;

/// Concept defining the interface for all store types.
template <typename T>
concept StoreConcept
    = requires(T& store, const T& constStore, TupleBuffer buffer, TupleBuffer& bufferRef, const Schema& schema, Store& self) {
          { store.open() };
          { store.close(self) };
          { store.flush(self) };

          /// Write a TupleBuffer to the store
          { store.write(std::move(buffer), schema, self) };

          /// Read data into a TupleBuffer, return number of rows written
          { store.read(bufferRef, schema) } -> std::convertible_to<uint64_t>;

          /// Check if there is more data to read
          { constStore.hasMore() } -> std::convertible_to<bool>;

          /// Get the schema associated with this store
          { constStore.getSchema() } -> std::convertible_to<Schema>;

          /// Get the current size in bytes of stored data
          { constStore.size() } -> std::convertible_to<uint64_t>;

          /// Get a human-readable type name
          { constStore.typeName() } noexcept -> std::convertible_to<std::string_view>;
      };

namespace detail
{

/// Type-erased virtual base for stores (hidden from users).
struct ErasedStore
{
    virtual ~ErasedStore() = default;

    virtual void open() = 0;
    virtual void close(Store& self) = 0;
    virtual void flush(Store& self) = 0;
    virtual void write(TupleBuffer buffer, const Schema& schema, Store& self) = 0;
    [[nodiscard]] virtual uint64_t read(TupleBuffer& buffer, const Schema& schema) = 0;
    [[nodiscard]] virtual bool hasMore() const = 0;
    [[nodiscard]] virtual Schema getSchema() const = 0;
    [[nodiscard]] virtual uint64_t size() const = 0;
    [[nodiscard]] virtual std::string_view typeName() const noexcept = 0;
};

/// Bridge wrapping a concrete store type satisfying StoreConcept.
template <StoreConcept StoreType>
struct StoreModel final : ErasedStore
{
    StoreType impl;

    /// Construct the wrapped store in-place from forwarded arguments.
    template <typename... Args>
    explicit StoreModel(std::in_place_t, Args&&... args) : impl(std::forward<Args>(args)...)
    {
    }

    void open() override { impl.open(); }

    void close(Store& self) override { impl.close(self); }

    void flush(Store& self) override { impl.flush(self); }

    void write(TupleBuffer buffer, const Schema& schema, Store& self) override { impl.write(std::move(buffer), schema, self); }

    [[nodiscard]] uint64_t read(TupleBuffer& buffer, const Schema& schema) override { return impl.read(buffer, schema); }

    [[nodiscard]] bool hasMore() const override { return impl.hasMore(); }

    [[nodiscard]] Schema getSchema() const override { return impl.getSchema(); }

    [[nodiscard]] uint64_t size() const override { return impl.size(); }

    [[nodiscard]] std::string_view typeName() const noexcept override { return impl.typeName(); }
};

}

/// Type-erased wrapper for heap-allocated stores.
/// @tparam Checked Either detail::ErasedStore (type-erased) or a concrete store type.
template <typename Checked>
struct TypedStore
{
    /// Construct from a typed store of a specific concrete type.
    template <StoreConcept T>
    requires std::same_as<Checked, detail::ErasedStore>
    TypedStore(TypedStore<T> other) : self(std::move(other.self)) /// NOLINT(google-explicit-constructor)
    {
    }

    explicit TypedStore(std::shared_ptr<detail::ErasedStore> store) : self(std::move(store)) { }

    TypedStore() = delete;

    /// Attempt to get the underlying store as type T.
    template <StoreConcept T>
    [[nodiscard]] std::optional<TypedStore<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<detail::StoreModel<T>>(self))
        {
            return TypedStore<T>{std::static_pointer_cast<detail::ErasedStore>(model)};
        }
        return std::nullopt;
    }

    /// Get the underlying store as type T. Throws on mismatch.
    template <StoreConcept T>
    [[nodiscard]] TypedStore<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<detail::StoreModel<T>>(self))
        {
            return TypedStore<T>{std::static_pointer_cast<detail::ErasedStore>(model)};
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", NAMEOF_TYPE(T), self->typeName());
        std::unreachable();
    }

    /// Access the concrete store directly (only valid when Checked is a concrete type).
    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<detail::ErasedStore, Checked>)
        {
            return *self;
        }
        else
        {
            return std::dynamic_pointer_cast<detail::StoreModel<Checked>>(self)->impl;
        }
    }

    /// Mutable access to the concrete store.
    [[nodiscard]] Checked& getMutable()
    {
        if constexpr (std::is_same_v<detail::ErasedStore, Checked>)
        {
            return *self;
        }
        else
        {
            return std::dynamic_pointer_cast<detail::StoreModel<Checked>>(self)->impl;
        }
    }

    /// Delegating methods
    void open() { self->open(); }

    void close()
    {
        Store selfStore(self);
        self->close(selfStore);
    }

    void flush()
    {
        Store selfStore(self);
        self->flush(selfStore);
    }

    void write(TupleBuffer buffer, const Schema& schema)
    {
        Store selfStore(self);
        self->write(std::move(buffer), schema, selfStore);
    }

    [[nodiscard]] uint64_t read(TupleBuffer& buffer, const Schema& schema) { return self->read(buffer, schema); }

    [[nodiscard]] bool hasMore() const { return self->hasMore(); }

    [[nodiscard]] Schema getSchema() const { return self->getSchema(); }

    [[nodiscard]] uint64_t size() const { return self->size(); }

    [[nodiscard]] std::string_view typeName() const noexcept { return self->typeName(); }

private:
    template <typename FriendChecked>
    friend struct TypedStore;

    std::shared_ptr<detail::ErasedStore> self;
};

/// Construct a type-erased Store by building the concrete store in-place.
/// Usage: auto store = makeStore<MemoryStore>(schema);
template <StoreConcept T, typename... Args>
Store makeStore(Args&&... args)
{
    return Store(std::make_shared<detail::StoreModel<T>>(std::in_place, std::forward<Args>(args)...));
}

}
