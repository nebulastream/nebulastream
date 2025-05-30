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

/// std::ranges::to
/// Libc++ already provides an implementation for std::ranges::to so the implementation is libstdc++ specific
#ifndef _LIBCPP_VERSION
namespace ranges
{
namespace detail
{
template <typename Container>
constexpr bool reservable_container
    = std::ranges::sized_range<Container> && requires(Container& c, std::ranges::range_size_t<Container> n) {
          c.reserve(n);
          { c.capacity() } -> std::same_as<decltype(n)>;
          { c.max_size() } -> std::same_as<decltype(n)>;
      };

template <typename ContainerType, typename RangeType>
constexpr bool toable = requires {
    requires(
        !std::ranges::input_range<ContainerType>
        || std::convertible_to<std::ranges::range_reference_t<RangeType>, std::ranges::range_value_t<ContainerType>>);
};
}

struct from_range_t
{
    explicit from_range_t() = default;
};
inline constexpr from_range_t from_range{};

/// @cond undocumented
namespace detail
{
template <typename _Rg, typename Tp>
concept container_compatible_range = std::ranges::input_range<_Rg> && std::convertible_to<std::ranges::range_reference_t<_Rg>, Tp>;
}

template <typename IterType>
using iter_category = typename std::iterator_traits<IterType>::iterator_category;

template <typename ContainerType, std::ranges::input_range RangeType, typename... Args>
requires(!std::ranges::view<ContainerType>)
constexpr ContainerType to [[nodiscard]] (RangeType&& r, Args&&... args)
{
    static_assert(!std::is_const_v<ContainerType> && !std::is_volatile_v<ContainerType>);
    static_assert(std::is_class_v<ContainerType>);

    if constexpr (detail::toable<ContainerType, RangeType>)
    {
        if constexpr (std::constructible_from<ContainerType, RangeType, Args...>)
            return ContainerType(std::forward<RangeType>(r), std::forward<Args>(args)...);
        else if constexpr (std::constructible_from<ContainerType, from_range_t, RangeType, Args...>)
            return ContainerType(from_range, std::forward<RangeType>(r), std::forward<Args>(args)...);
        else if constexpr (requires {
                               requires std::ranges::common_range<RangeType>;
                               typename iter_category<std::ranges::iterator_t<RangeType>>;
                               requires std::derived_from<iter_category<std::ranges::iterator_t<RangeType>>, std::input_iterator_tag>;
                               requires std::constructible_from<
                                   ContainerType,
                                   std::ranges::iterator_t<RangeType>,
                                   std::ranges::sentinel_t<RangeType>,
                                   Args...>;
                           })
            return ContainerType(std::ranges::begin(r), std::ranges::end(r), std::forward<Args>(args)...);
        else
        {
            static_assert(std::constructible_from<ContainerType, Args...>);
            ContainerType c(std::forward<Args>(args)...);
            if constexpr (std::ranges::sized_range<RangeType> && detail::reservable_container<ContainerType>)
                c.reserve(static_cast<std::ranges::range_size_t<ContainerType>>(std::ranges::size(r)));
            auto it = std::ranges::begin(r);
            const auto sent = std::ranges::end(r);
            while (it != sent)
            {
                if constexpr (requires { c.emplace_back(*it); })
                    c.emplace_back(*it);
                else if constexpr (requires { c.push_back(*it); })
                    c.push_back(*it);
                else if constexpr (requires { c.emplace(c.end(), *it); })
                    c.emplace(c.end(), *it);
                else
                    c.insert(c.end(), *it);
                ++it;
            }
            return c;
        }
    }
    else
    {
        static_assert(std::ranges::input_range<std::ranges::range_reference_t<RangeType>>);
        return ranges::to<ContainerType>(
            ref_view(r)
                | std::views::transform(
                    []<typename _Elt>(_Elt&& elem)
                    {
                        using _ValT = std::ranges::range_value_t<ContainerType>;
                        return ranges::to<_ValT>(std::forward<_Elt>(elem));
                    }),
            std::forward<Args>(args)...);
    }
}

namespace detail
{
template <typename RangeType>
struct _InputIter
{
    using iterator_category = std::input_iterator_tag;
    using value_type = std::ranges::range_value_t<RangeType>;
    using difference_type = ptrdiff_t;
    using pointer = std::add_pointer_t<std::ranges::range_reference_t<RangeType>>;
    using reference = std::ranges::range_reference_t<RangeType>;
    reference operator*() const;
    pointer operator->() const;
    _InputIter& operator++();
    _InputIter operator++(int);
    bool operator==(const _InputIter&) const;
};

template <template <typename...> typename ContainerType, std::ranges::input_range RangeType, typename... Args>
using DeducerExpre1 = decltype(ContainerType(std::declval<RangeType>(), std::declval<Args>()...));

template <template <typename...> typename ContainerType, std::ranges::input_range RangeType, typename... Args>
using DeducerExpre2 = decltype(ContainerType(from_range, std::declval<RangeType>(), std::declval<Args>()...));

template <template <typename...> typename ContainerType, std::ranges::input_range RangeType, typename... Args>
using DeducerExpre3
    = decltype(ContainerType(std::declval<_InputIter<RangeType>>(), std::declval<_InputIter<RangeType>>(), std::declval<Args>()...));

}

template <template <typename...> typename ContainerType, std::ranges::input_range RangeType, typename... Args>
constexpr auto to [[nodiscard]] (RangeType&& r, Args&&... args)
{
    using detail::DeducerExpre1;
    using detail::DeducerExpre2;
    using detail::DeducerExpre3;
    if constexpr (requires { typename DeducerExpre1<ContainerType, RangeType, Args...>; })
        return ranges::to<DeducerExpre1<ContainerType, RangeType, Args...>>(std::forward<RangeType>(r), std::forward<Args>(args)...);
    else if constexpr (requires { typename DeducerExpre2<ContainerType, RangeType, Args...>; })
        return ranges::to<DeducerExpre2<ContainerType, RangeType, Args...>>(std::forward<RangeType>(r), std::forward<Args>(args)...);
    else if constexpr (requires { typename DeducerExpre3<ContainerType, RangeType, Args...>; })
        return ranges::to<DeducerExpre3<ContainerType, RangeType, Args...>>(std::forward<RangeType>(r), std::forward<Args>(args)...);
    else
        static_assert(false); /// Cannot deduce container specialization.
}

namespace detail
{
template <typename ContainerType>
struct To
{
    template <typename RangeType, typename... Args>
    requires requires { ranges::to<ContainerType>(std::declval<RangeType>(), std::declval<Args>()...); }
    constexpr auto operator()(RangeType&& r, Args... args) const
    {
        return ranges::to<ContainerType>(std::forward<RangeType>(r), std::forward<Args>(args)...);
    }
};
}

template <typename ContainerType, typename... Args>
requires(!std::ranges::view<ContainerType>)
constexpr auto to [[nodiscard]] (Args&&... args)
{
    using detail::To;
    return views::adaptor::Partial<To<ContainerType>, std::decay_t<Args>...>{0, std::forward<Args>(args)...};
}

namespace detail
{
template <template <typename...> typename ContainerType>
struct To2
{
    template <typename RangeType, typename... Args>
    requires requires { ranges::to<ContainerType>(std::declval<RangeType>(), std::declval<Args>()...); }
    constexpr auto operator()(RangeType&& r, Args... args) const
    {
        return ranges::to<ContainerType>(std::forward<RangeType>(r), std::forward<Args>(args)...);
    }
};
}

template <template <typename...> typename ContainerType, typename... Args>
constexpr auto to [[nodiscard]] (Args&&... args)
{
    using detail::To2;
    return views::adaptor::Partial<To2<ContainerType>, std::decay_t<Args>...>{0, std::forward<Args>(args)...};
}
}

///NOLINTEND
