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
#include <memory>
#include <string_view>
#include <utility>

#include <Store.hpp>

namespace NES::StoreManager
{

namespace detail
{
struct ErasedStoreTransformation;
}

template <typename Checked = detail::ErasedStoreTransformation>
struct TypedStoreTransformation;
using StoreTransformation = TypedStoreTransformation<>;

/// Concept defining the interface for transformations between store types.
template <typename T>
concept StoreTransformationConcept = requires(T& t, Store& source, Store& dest) {
    { t.execute(source, dest) };
    { t.name() } noexcept -> std::convertible_to<std::string_view>;
};

namespace detail
{

struct ErasedStoreTransformation
{
    virtual ~ErasedStoreTransformation() = default;
    virtual void execute(Store& source, Store& dest) = 0;
    [[nodiscard]] virtual std::string_view name() const noexcept = 0;
};

template <StoreTransformationConcept TransformationType>
struct StoreTransformationModel final : ErasedStoreTransformation
{
    TransformationType impl;

    explicit StoreTransformationModel(TransformationType impl) : impl(std::move(impl)) { }

    void execute(Store& source, Store& dest) override { impl.execute(source, dest); }

    [[nodiscard]] std::string_view name() const noexcept override { return impl.name(); }
};

}

template <typename Checked>
struct TypedStoreTransformation
{
    template <StoreTransformationConcept T>
    TypedStoreTransformation(T transformation) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<detail::StoreTransformationModel<T>>(std::move(transformation)))
    {
    }

    template <StoreTransformationConcept T>
    requires std::same_as<Checked, detail::ErasedStoreTransformation>
    TypedStoreTransformation(TypedStoreTransformation<T> other) : self(std::move(other.self)) /// NOLINT(google-explicit-constructor)
    {
    }

    explicit TypedStoreTransformation(std::shared_ptr<detail::ErasedStoreTransformation> t) : self(std::move(t)) { }

    TypedStoreTransformation() = delete;

    void execute(Store& source, Store& dest) { self->execute(source, dest); }

    [[nodiscard]] std::string_view name() const noexcept { return self->name(); }

private:
    template <typename FriendChecked>
    friend struct TypedStoreTransformation;

    std::shared_ptr<detail::ErasedStoreTransformation> self;
};

}
