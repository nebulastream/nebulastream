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

#include <compare>
#include <concepts>
#include <functional>
#include <iterator>
#include <ranges>
#include <tuple>
#include <type_traits>

/// This adds commonly used ranges methods, which are not currently supported by both libc++20 and libstdc++14
/// Source is taken from upstream implementation of libc++ and adopted to not use reserved identifier names and not collide with
/// declarations within the std namespace.
///
/// Currently, we implement:
///     std::views::enumerate (https://en.cppreference.com/w/cpp/ranges/enumerate_view). Available as NES::views::enumerate

/// NOLINTBEGIN
///
namespace NES::views
{
///taken from libcxx/include/__functional/perfect_forward.h
///https://github.com/llvm/llvm-project/blob/a481452cd88acc180f82dd5631257c8954ed7812/libcxx/include/__functional/perfect_forward.h#L49
template <class Op, class Indicies, class... BoundArgs>
struct PerfectForwardImpl;

template <class Op, size_t... Idx, class... BoundArgs>
struct PerfectForwardImpl<Op, std::index_sequence<Idx...>, BoundArgs...>
{
private:
    std::tuple<BoundArgs...> bound_args_;

public:
    template <class... Args, class = std::enable_if_t<std::is_constructible_v<std::tuple<BoundArgs...>, Args&&...>>>
    explicit constexpr PerfectForwardImpl(Args&&... bound_args) : bound_args_(std::forward<Args>(bound_args)...)
    {
    }

    PerfectForwardImpl(const PerfectForwardImpl&) = default;
    PerfectForwardImpl(PerfectForwardImpl&&) = default;

    PerfectForwardImpl& operator=(const PerfectForwardImpl&) = default;
    PerfectForwardImpl& operator=(PerfectForwardImpl&&) = default;

    template <class... Args, class = std::enable_if_t<std::is_invocable_v<Op, BoundArgs&..., Args...>>>
    constexpr auto operator()(Args&&... args) & noexcept(noexcept(Op()(std::get<Idx>(bound_args_)..., std::forward<Args>(args)...)))
        -> decltype(Op()(std::get<Idx>(bound_args_)..., std::forward<Args>(args)...))
    {
        return Op()(std::get<Idx>(bound_args_)..., std::forward<Args>(args)...);
    }

    template <class... Args, class = std::enable_if_t<!std::is_invocable_v<Op, BoundArgs&..., Args...>>>
    auto operator()(Args&&...) & = delete;

    template <class... Args, class = std::enable_if_t<std::is_invocable_v<Op, const BoundArgs&..., Args...>>>
    constexpr auto operator()(Args&&... args) const& noexcept(noexcept(Op()(std::get<Idx>(bound_args_)..., std::forward<Args>(args)...)))
        -> decltype(Op()(std::get<Idx>(bound_args_)..., std::forward<Args>(args)...))
    {
        return Op()(std::get<Idx>(bound_args_)..., std::forward<Args>(args)...);
    }

    template <class... Args, class = std::enable_if_t<!std::is_invocable_v<Op, const BoundArgs&..., Args...>>>
    auto operator()(Args&&...) const& = delete;

    template <class... Args, class = std::enable_if_t<std::is_invocable_v<Op, BoundArgs..., Args...>>>
    constexpr auto
    operator()(Args&&... args) && noexcept(noexcept(Op()(std::get<Idx>(std::move(bound_args_))..., std::forward<Args>(args)...)))
        -> decltype(Op()(std::get<Idx>(std::move(bound_args_))..., std::forward<Args>(args)...))
    {
        return Op()(std::get<Idx>(std::move(bound_args_))..., std::forward<Args>(args)...);
    }

    template <class... Args, class = std::enable_if_t<!std::is_invocable_v<Op, BoundArgs..., Args...>>>
    auto operator()(Args&&...) && = delete;

    template <class... Args, class = std::enable_if_t<std::is_invocable_v<Op, const BoundArgs..., Args...>>>
    constexpr auto
    operator()(Args&&... args) const&& noexcept(noexcept(Op()(std::get<Idx>(std::move(bound_args_))..., std::forward<Args>(args)...)))
        -> decltype(Op()(std::get<Idx>(std::move(bound_args_))..., std::forward<Args>(args)...))
    {
        return Op()(std::get<Idx>(std::move(bound_args_))..., std::forward<Args>(args)...);
    }

    template <class... Args, class = std::enable_if_t<!std::is_invocable_v<Op, const BoundArgs..., Args...>>>
    auto operator()(Args&&...) const&& = delete;
};

/// PerfectForward implements a perfect-forwarding call wrapper as explained in [func.require].
template <class Op, class... Args>
using PerfectForward = PerfectForwardImpl<Op, std::index_sequence_for<Args...>, Args...>;


///taken from libcxx/include/__functional/compose.h
///https://github.com/llvm/llvm-project/blob/a481452cd88acc180f82dd5631257c8954ed7812/libcxx/include/__functional/compose.h#L27
struct ComposeOp
{
    template <class Fn1, class Fn2, class... Args>
    constexpr auto operator()(Fn1&& f1, Fn2&& f2, Args&&... args) const
        noexcept(noexcept(std::invoke(std::forward<Fn1>(f1), std::invoke(std::forward<Fn2>(f2), std::forward<Args>(args)...))))
            -> decltype(std::invoke(std::forward<Fn1>(f1), std::invoke(std::forward<Fn2>(f2), std::forward<Args>(args)...)))
    {
        return std::invoke(std::forward<Fn1>(f1), std::invoke(std::forward<Fn2>(f2), std::forward<Args>(args)...));
    }
};

template <class Fn1, class Fn2>
struct Compose : PerfectForward<ComposeOp, Fn1, Fn2>
{
    using PerfectForward<ComposeOp, Fn1, Fn2>::PerfectForward;
};

template <class Fn1, class Fn2>
constexpr auto
compose(Fn1&& f1, Fn2&& f2) noexcept(noexcept(Compose<std::decay_t<Fn1>, std::decay_t<Fn2>>(std::forward<Fn1>(f1), std::forward<Fn2>(f2))))
    -> decltype(Compose<std::decay_t<Fn1>, std::decay_t<Fn2>>(std::forward<Fn1>(f1), std::forward<Fn2>(f2)))
{
    return Compose<std::decay_t<Fn1>, std::decay_t<Fn2>>(std::forward<Fn1>(f1), std::forward<Fn2>(f2));
}


///taken from libcxx/include/__ranges/range_adaptor.h

/// CRTP base that one can derive from in order to be considered a range adaptor closure
/// by the library. When deriving from this class, a pipe operator will be provided to
/// make the following hold:
/// - `x | f` is equivalent to `f(x)`
/// - `f1 | f2` is an adaptor closure `g` such that `g(x)` is equivalent to `f2(f1(x))`
template <class T>
requires std::is_class_v<T> && std::same_as<T, std::remove_cv_t<T>>
struct range_adaptor_closure
{
};

/// Type that wraps an arbitrary function object and makes it into a range adaptor closure,
/// i.e. something that can be called via the `x | f` notation.
template <class _Fn>
struct pipeable : _Fn, range_adaptor_closure<pipeable<_Fn>>
{
    constexpr explicit pipeable(_Fn&& f) : _Fn(std::move(f)) { }
};

template <class T>
T derived_from_range_adaptor_closure(range_adaptor_closure<T>*);

template <class T>
concept RangeAdaptorClosure = !std::ranges::range<std::remove_cvref_t<T>> && requires {
    /// Ensure that `remove_cvref_t<T>` is derived from `range_adaptor_closure<remove_cvref_t<T>>` and isn't derived
    /// from `range_adaptor_closure<U>` for any other type `U`.
    { derived_from_range_adaptor_closure((std::remove_cvref_t<T>*)nullptr) } -> std::same_as<std::remove_cvref_t<T>>;
};

template <std::ranges::range Range, RangeAdaptorClosure _Closure>
requires std::invocable<_Closure, Range>
[[nodiscard]] constexpr decltype(auto) operator|(Range&& range, _Closure&& closure) noexcept(std::is_nothrow_invocable_v<_Closure, Range>)
{
    return std::invoke(std::forward<_Closure>(closure), std::forward<Range>(range));
}

template <RangeAdaptorClosure _Closure, RangeAdaptorClosure _OtherClosure>
requires std::constructible_from<std::decay_t<_Closure>, _Closure> && std::constructible_from<std::decay_t<_OtherClosure>, _OtherClosure>
[[nodiscard]] constexpr auto operator|(_Closure&& c1, _OtherClosure&& c2) noexcept(
    std::is_nothrow_constructible_v<std::decay_t<_Closure>, _Closure>
    && std::is_nothrow_constructible_v<std::decay_t<_OtherClosure>, _OtherClosure>)
{
    return pipeable(compose(std::forward<_OtherClosure>(c2), std::forward<_Closure>(c1)));
}


/// [range.enumerate.view]

///taken from libcxx include/__type_traits/maybe_const.h
///https://github.com/llvm/llvm-project/blob/212a48b4daf3101871ba6e7c47cf103df66a5e56/libcxx/include/__type_traits/maybe_const.h#L22
template <bool Const, class T>
using maybe_const = std::conditional_t<Const, const T, T>;

///taken from libcxx include/__ranges/concepts.h
///https://github.com/llvm/llvm-project/blob/212a48b4daf3101871ba6e7c47cf103df66a5e56/libcxx/include/__ranges/concepts.h#L97
template <class T>
concept view = std::ranges::range<T> && std::movable<T> && std::ranges::enable_view<T>;

///rest taken from draft PR for ranges enumerate #73617
///https://github.com/llvm/llvm-project/blob/d98841ba1df1fd71a75194ba7c570fe3d43882a3/libcxx/include/__ranges/enumerate_view.h
template <class R>
concept range_with_movable_references = std::ranges::input_range<R> && std::move_constructible<std::ranges::range_reference_t<R>>
    && std::move_constructible<std::ranges::range_rvalue_reference_t<R>>;

template <class Range>
concept simple_view
    = view<Range> && std::ranges::range<const Range> && std::same_as<std::ranges::iterator_t<Range>, std::ranges::iterator_t<const Range>>
    && std::same_as<std::ranges::sentinel_t<Range>, std::ranges::sentinel_t<const Range>>;
template <view View>
requires range_with_movable_references<View>
class enumerate_view : public std::ranges::view_interface<enumerate_view<View>>
{
    View base_ = View();

    /// [range.enumerate.iterator]
    template <bool Const>
    class iterator;

    /// [range.enumerate.sentinel]
    template <bool Const>
    class sentinel;

public:
    constexpr enumerate_view()
    requires std::default_initializable<View>
    = default;
    constexpr explicit enumerate_view(View base) : base_(std::move(base)) { }

    [[nodiscard]] constexpr auto begin()
    requires(!simple_view<View>)
    {
        return iterator<false>(std::ranges::begin(base_), 0);
    }
    [[nodiscard]] constexpr auto begin() const
    requires range_with_movable_references<const View>
    {
        return iterator<true>(std::ranges::begin(base_), 0);
    }

    [[nodiscard]] constexpr auto end()
    requires(!simple_view<View>)
    {
        if constexpr (std::ranges::forward_range<View> && std::ranges::common_range<View> && std::ranges::sized_range<View>)
            return iterator<false>(std::ranges::end(base_), std::ranges::distance(base_));
        else
            return sentinel<false>(std::ranges::end(base_));
    }
    [[nodiscard]] constexpr auto end() const
    requires range_with_movable_references<const View>
    {
        if constexpr (std::ranges::forward_range<View> && std::ranges::common_range<const View> && std::ranges::sized_range<const View>)
            return iterator<true>(std::ranges::end(base_), std::ranges::distance(base_));
        else
            return sentinel<true>(std::ranges::end(base_));
    }

    [[nodiscard]] constexpr auto size()
    requires std::ranges::sized_range<View>
    {
        return std::ranges::size(base_);
    }
    [[nodiscard]] constexpr auto size() const
    requires std::ranges::sized_range<const View>
    {
        return std::ranges::size(base_);
    }

    [[nodiscard]] constexpr View base() const&
    requires std::copy_constructible<View>
    {
        return base_;
    }
    [[nodiscard]] constexpr View base() && { return std::move(base_); }
};

template <class Range>
enumerate_view(Range&&) -> enumerate_view<std::views::all_t<Range>>;

/// [range.enumerate.iterator]

template <view View>
requires range_with_movable_references<View>
template <bool Const>
class enumerate_view<View>::iterator
{
    using Base = maybe_const<Const, View>;

    static consteval auto get_iterator_concept()
    {
        if constexpr (std::ranges::random_access_range<Base>)
        {
            return std::random_access_iterator_tag{};
        }
        else if constexpr (std::ranges::bidirectional_range<Base>)
        {
            return std::bidirectional_iterator_tag{};
        }
        else if constexpr (std::ranges::forward_range<Base>)
        {
            return std::forward_iterator_tag{};
        }
        else
        {
            return std::input_iterator_tag{};
        }
    }

    friend class enumerate_view<View>;

public:
    using iterator_category = std::input_iterator_tag;
    using iterator_concept = decltype(get_iterator_concept());
    using difference_type = std::ranges::range_difference_t<Base>;
    using value_type = std::tuple<difference_type, std::ranges::range_value_t<Base>>;

private:
    using reference_type = std::tuple<difference_type, std::ranges::range_reference_t<Base>>;

    std::ranges::iterator_t<Base> current_ = std::ranges::iterator_t<Base>();
    difference_type pos_ = 0;

    constexpr explicit iterator(std::ranges::iterator_t<Base> current, difference_type pos) : current_(std::move(current)), pos_(pos) { }

public:
    iterator()
    requires std::default_initializable<std::ranges::iterator_t<Base>>
    = default;

    constexpr iterator(iterator<!Const> iterator)
    requires Const && std::convertible_to<std::ranges::iterator_t<View>, std::ranges::iterator_t<Base>>
        : current_(std::move(iterator.current_)), pos_(iterator.pos_)
    {
    }

    [[nodiscard]] constexpr const std::ranges::iterator_t<Base>& base() const& noexcept { return current_; }

    [[nodiscard]] constexpr std::ranges::iterator_t<Base> base() && { return std::move(current_); }

    [[nodiscard]] constexpr difference_type index() const noexcept { return pos_; }

    [[nodiscard]] constexpr auto operator*() const { return reference_type(pos_, *current_); }

    constexpr iterator& operator++()
    {
        ++current_;
        ++pos_;
        return *this;
    }

    constexpr void operator++(int) { return ++*this; }

    constexpr iterator operator++(int)
    requires std::ranges::forward_range<Base>
    {
        auto temp = *this;
        ++*this;
        return temp;
    }

    constexpr iterator& operator--()
    requires std::ranges::bidirectional_range<Base>
    {
        --current_;
        --pos_;
        return *this;
    }

    constexpr iterator operator--(int)
    requires std::ranges::bidirectional_range<Base>
    {
        auto temp = *this;
        --*this;
        return *temp;
    }

    constexpr iterator& operator+=(difference_type n)
    requires std::ranges::random_access_range<Base>
    {
        current_ += n;
        pos_ += n;
        return *this;
    }

    constexpr iterator& operator-=(difference_type n)
    requires std::ranges::random_access_range<Base>
    {
        current_ -= n;
        pos_ -= n;
        return *this;
    }

    [[nodiscard]] constexpr auto operator[](difference_type n) const
    requires std::ranges::random_access_range<Base>
    {
        return reference_type(pos_ + n, current_[n]);
    }

    [[nodiscard]] friend constexpr bool operator==(const iterator& x, const iterator& y) noexcept { return x.pos_ == y.pos_; }

    [[nodiscard]] friend constexpr std::strong_ordering operator<=>(const iterator& x, const iterator& y) noexcept
    {
        return x.pos_ <=> y.pos_;
    }

    [[nodiscard]] friend constexpr iterator operator+(const iterator& i, difference_type n)
    requires std::ranges::random_access_range<Base>
    {
        auto temp = i;
        temp += n;
        return temp;
    }

    [[nodiscard]] friend constexpr iterator operator+(difference_type n, const iterator& i)
    requires std::ranges::random_access_range<Base>
    {
        return i + n;
    }

    [[nodiscard]] friend constexpr iterator operator-(const iterator& i, difference_type n)
    requires std::ranges::random_access_range<Base>
    {
        auto temp = i;
        temp -= n;
        return temp;
    }

    [[nodiscard]] friend constexpr difference_type operator-(const iterator& x, const iterator& y) noexcept { return x.pos_ - y.pos_; }

    [[nodiscard]] friend constexpr auto iter_move(const iterator& i) noexcept(
        noexcept(std::ranges::iter_move(i.current_)) && std::is_nothrow_move_constructible_v<std::ranges::range_rvalue_reference_t<Base>>)
    {
        return std::tuple<difference_type, std::ranges::range_rvalue_reference_t<Base>>(i.pos_, std::ranges::iter_move(i.current_));
    }
};

/// [range.enumerate.sentinel]
template <view View>
requires range_with_movable_references<View>
template <bool Const>
class enumerate_view<View>::sentinel
{
    using Base = maybe_const<Const, View>;

    std::ranges::sentinel_t<Base> end = std::ranges::sentinel_t<Base>();

    constexpr explicit sentinel(std::ranges::sentinel_t<Base> end) : end(std::move(end)) { }

    friend class enumerate_view<View>;

public:
    sentinel() = default;

    constexpr sentinel(sentinel<!Const> other)
    requires Const && std::convertible_to<std::ranges::sentinel_t<View>, std::ranges::sentinel_t<Base>>
        : end(std::move(other.end))
    {
    }

    [[nodiscard]] constexpr std::ranges::sentinel_t<Base> base() const { return end; }

    template <bool OtherConst>
    requires std::sentinel_for<std::ranges::sentinel_t<Base>, std::ranges::iterator_t<maybe_const<OtherConst, View>>>
    [[nodiscard]] friend constexpr bool operator==(const iterator<OtherConst>& x, const sentinel& y)
    {
        return x.current_ == y.end;
    }

    template <bool OtherConst>
    requires std::sized_sentinel_for<std::ranges::sentinel_t<Base>, std::ranges::iterator_t<maybe_const<OtherConst, View>>>
    [[nodiscard]] friend constexpr std::ranges::range_difference_t<maybe_const<OtherConst, View>>
    operator-(const iterator<OtherConst>& x, const sentinel& y)
    {
        return x.current_ - y.end;
    }

    template <bool OtherConst>
    requires std::sized_sentinel_for<std::ranges::sentinel_t<Base>, std::ranges::iterator_t<maybe_const<OtherConst, View>>>
    [[nodiscard]] friend constexpr std::ranges::range_difference_t<maybe_const<OtherConst, View>>
    operator-(const sentinel& x, const iterator<OtherConst>& y)
    {
        return x.end - y.current_;
    }
};
}


template <class View>
constexpr bool std::ranges::enable_borrowed_range<NES::views::enumerate_view<View>> = std::ranges::enable_borrowed_range<View>;

namespace NES::views
{
struct fn : range_adaptor_closure<fn>
{
    template <class Range>
    [[nodiscard]] constexpr auto operator()(Range&& range) const noexcept(noexcept(/**/ enumerate_view(std::forward<Range>(range))))
        -> decltype(/*--*/ enumerate_view(std::forward<Range>(range)))
    {
        return /*-------------*/ enumerate_view(std::forward<Range>(range));
    }
};
inline constexpr auto enumerate = fn{};

}

///NOLINTEND
