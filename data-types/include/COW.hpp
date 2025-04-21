//
// Created by ls on 4/18/25.
//

#pragma once
#include <concepts>
#include <memory>
#include <type_traits>
#include <variant>

namespace NES
{
template <typename T>
concept Cloneable = requires(const T& t) {
    {
        t.clone()
    } -> std::same_as<std::shared_ptr<T>>;
};

template <Cloneable T>
class COW
{
    mutable std::variant<std::shared_ptr<const T>, std::shared_ptr<T>> impl;

    T& mutable_ref()
    {
        std::visit(
            [this]<typename PtrType>(PtrType& ptr)
            {
                if constexpr (std::is_const<typename PtrType::element_type>::value)
                {
                    if (ptr.use_count() == 1)
                    {
                        this->impl = std::const_pointer_cast<T>(ptr);
                    }
                    else
                    {
                        this->impl = ptr->clone();
                    }
                }
            },
            impl);
        return *std::get<std::shared_ptr<T>>(this->impl);
    }

public:
    T& operator*() { return mutable_ref(); }
    T* operator->() { return &mutable_ref(); }

    const T& operator*() const
    {
        return std::visit([](const auto& ptr) -> const T& { return std::const_pointer_cast<const T>(ptr).operator*(); }, impl);
    }

    const T* operator->() const
    {
        return std::visit([](const auto& ptr) -> const T* { return std::const_pointer_cast<const T>(ptr).operator->(); }, impl);
    }

    template <typename... Args>
    COW(Args... args) : impl(std::make_shared<T>(std::forward<Args>(args)...))
    {
    }
    COW(std::shared_ptr<T> ptr) : impl(std::move(ptr)) { }
    COW(std::shared_ptr<const T> ptr) : impl(std::move(ptr)) { }

    COW(std::convertible_to<std::shared_ptr<T>> auto ptr) : impl(std::move(static_cast<std::shared_ptr<T>>(ptr))) { }

    COW(const COW& other)
    {
        other.impl = std::visit([](auto& ptr) { return std::const_pointer_cast<const T>(ptr); }, other.impl);
        impl = other.impl;
    }

    COW(COW&& other) : impl(std::move(other.impl)) { }

    COW& operator=(const COW& other)
    {
        other.impl = std::visit([](auto& ptr) { return std::const_pointer_cast<const T>(ptr); }, other.impl);
        impl = other.impl;
        return *this;
    }

    COW& operator=(COW&& other)
    {
        impl = std::move(other.impl);
        return *this;
    }
};

}