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

/*namespace NES
{
/// This adds commonly used ranges methods, which are not currently supported by both libc++ 18 and libstdc++ 13
/// Source is taken from upstream implementation of libstdc++ and adopted to not use reserved identifier names and not collide with
/// declarations within the std namespace.
///
/// Currently, we implement:
///     std::views::enumerate (https://en.cppreference.com/w/cpp/ranges/enumerate_view). Available as NES::views::enumerate
///     std::ranges::to (https://en.cppreference.com/w/cpp/ranges/to). Available as NES::ranges::enumerate

/// NOLINTBEGIN
namespace detail
{
template <typename Range>
concept range_with_movable_reference = std::ranges::input_range<Range> && std::move_constructible<std::ranges::range_reference_t<Range>>
    && std::move_constructible<std::ranges::range_rvalue_reference_t<Range>>;

template <typename Range>
concept simple_view = std::ranges::view<Range> && std::ranges::range<const Range>
    && std::same_as<std::ranges::iterator_t<Range>, std::ranges::iterator_t<const Range>>
    && std::same_as<std::ranges::sentinel_t<Range>, std::ranges::sentinel_t<const Range>>;

template <bool Const, typename T>
using maybe_const_t = std::conditional_t<Const, const T, T>;
}

template <std::ranges::view ViewType>
requires detail::range_with_movable_reference<ViewType>
class EnumerateView : public std::ranges::view_interface<EnumerateView<ViewType>>
{
    ViewType base_ = ViewType();

    template <bool IsConst>
    class IteratorType;
    template <bool IsConst>
    class SentinelType;

public:
    EnumerateView()
    requires std::default_initializable<ViewType>
    = default;

    constexpr explicit EnumerateView(ViewType base) : base_(std::move(base)) { }

    constexpr auto begin()
    requires(!detail::simple_view<ViewType>)
    {
        return IteratorType<false>(std::ranges::begin(base_), 0);
    }

    constexpr auto begin() const
    requires detail::range_with_movable_reference<const ViewType>
    {
        return IteratorType<true>(std::ranges::begin(base_), 0);
    }

    constexpr auto end()
    requires(!detail::simple_view<ViewType>)
    {
        if constexpr (std::ranges::common_range<ViewType> && std::ranges::sized_range<ViewType>)
        {
            return IteratorType<false>(std::ranges::end(base_), std::ranges::distance(base_));
        }
        else
        {
            return SentinelType<false>(std::ranges::end(base_));
        }
    }

    constexpr auto end() const
    requires detail::range_with_movable_reference<const ViewType>
    {
        if constexpr (std::ranges::common_range<const ViewType> && std::ranges::sized_range<const ViewType>)
        {
            return IteratorType<true>(std::ranges::end(base_), std::ranges::distance(base_));
        }
        else
        {
            return SentinelType<true>(std::ranges::end(base_));
        }
    }

    constexpr auto size()
    requires std::ranges::sized_range<ViewType>
    {
        return std::ranges::size(base_);
    }

    constexpr auto size() const
    requires std::ranges::sized_range<const ViewType>
    {
        return std::ranges::size(base_);
    }

    constexpr ViewType base() const&
    requires std::copy_constructible<ViewType>
    {
        return base_;
    }

    constexpr ViewType base() && { return std::move(base_); }
};

template <typename Range>
EnumerateView(Range&&) -> EnumerateView<std::ranges::views::all_t<Range>>;


template <std::ranges::view ViewType>
requires detail::range_with_movable_reference<ViewType>
template <bool IsConst>
class EnumerateView<ViewType>::IteratorType
{
    using BaseType = detail::maybe_const_t<IsConst, ViewType>;

    static auto IteratorConcept()
    {
        if constexpr (std::ranges::random_access_range<BaseType>)
        {
            return std::random_access_iterator_tag{};
        }
        else if constexpr (std::ranges::bidirectional_range<BaseType>)
        {
            return std::bidirectional_iterator_tag{};
        }
        else if constexpr (std::ranges::forward_range<BaseType>)
        {
            return std::forward_iterator_tag{};
        }
        else
        {
            return std::input_iterator_tag{};
        }
    }

    friend EnumerateView;

public:
    using iterator_category = std::input_iterator_tag;
    using iterator_concept = decltype(IteratorConcept());
    using difference_type = std::ranges::range_difference_t<BaseType>;
    using value_type = std::tuple<difference_type, std::ranges::range_value_t<BaseType>>;

private:
    using reference_type = std::tuple<difference_type, std::ranges::range_reference_t<BaseType>>;

    std::ranges::iterator_t<BaseType> current_ = std::ranges::iterator_t<BaseType>();
    difference_type pos_ = 0;

    constexpr explicit IteratorType(std::ranges::iterator_t<BaseType> current, difference_type pos)
        : current_(std::move(current)), pos_(pos)
    {
    }

public:
    IteratorType()
    requires std::default_initializable<std::ranges::iterator_t<BaseType>>
    = default;

    constexpr IteratorType(IteratorType<!IsConst> iterator)
    requires IsConst && std::convertible_to<std::ranges::iterator_t<ViewType>, std::ranges::iterator_t<BaseType>>
        : current_(std::move(iterator.current_)), pos_(iterator.pos_)
    {
    }

    constexpr const std::ranges::iterator_t<BaseType>& base() const& noexcept { return current_; }

    constexpr std::ranges::iterator_t<BaseType> base() && { return std::move(current_); }

    constexpr difference_type index() const noexcept { return pos_; }

    constexpr auto operator*() const { return reference_type(pos_, *current_); }

    constexpr IteratorType& operator++()
    {
        ++current_;
        ++pos_;
        return *this;
    }

    constexpr void operator++(int) { ++*this; }

    constexpr IteratorType operator++(int)
    requires std::ranges::forward_range<BaseType>
    {
        auto tmp = *this;
        ++*this;
        return tmp;
    }

    constexpr IteratorType& operator--()
    requires std::ranges::bidirectional_range<BaseType>
    {
        --current_;
        --pos_;
        return *this;
    }

    constexpr IteratorType operator--(int)
    requires std::ranges::bidirectional_range<BaseType>
    {
        auto tmp = *this;
        --*this;
        return tmp;
    }

    constexpr IteratorType& operator+=(difference_type distance)
    requires std::ranges::random_access_range<BaseType>
    {
        current_ += distance;
        pos_ += distance;
        return *this;
    }

    constexpr IteratorType& operator-=(difference_type distance)
    requires std::ranges::random_access_range<BaseType>
    {
        current_ -= distance;
        pos_ -= distance;
        return *this;
    }

    constexpr auto operator[](difference_type distance) const
    requires std::ranges::random_access_range<BaseType>
    {
        return reference_type(pos_ + distance, current_[distance]);
    }

    friend constexpr bool operator==(const IteratorType& lhs, const IteratorType& rhs) noexcept { return lhs.pos_ == rhs.pos_; }

    friend constexpr std::strong_ordering operator<=>(const IteratorType& lhs, const IteratorType& rhs) noexcept
    {
        return lhs.pos_ <=> rhs.pos_;
    }

    friend constexpr IteratorType operator+(const IteratorType& lhs, difference_type rhs)
    requires std::ranges::random_access_range<BaseType>
    {
        return (auto(lhs) += rhs);
    }

    friend constexpr IteratorType operator+(difference_type lhs, const IteratorType& rhs)
    requires std::ranges::random_access_range<BaseType>
    {
        return auto(rhs) += lhs;
    }

    friend constexpr IteratorType operator-(const IteratorType& lhs, difference_type rhs)
    requires std::ranges::random_access_range<BaseType>
    {
        return auto(lhs) -= rhs;
    }

    friend constexpr difference_type operator-(const IteratorType& lhs, const IteratorType& rhs) { return lhs.pos_ - rhs.pos_; }

    friend constexpr auto iter_move(const IteratorType& iterator) noexcept(
        noexcept(std::ranges::iter_move(iterator.current_))
        && std::is_nothrow_move_constructible_v<std::ranges::range_rvalue_reference_t<BaseType>>)
    {
        return std::tuple<difference_type, std::ranges::range_rvalue_reference_t<BaseType>>(
            iterator.pos_, std::ranges::iter_move(iterator.current_));
    }
};

template <std::ranges::view ViewType>
requires detail::range_with_movable_reference<ViewType>
template <bool IsConst>
class EnumerateView<ViewType>::SentinelType
{
    using BaseType = std::conditional_t<IsConst, const ViewType, ViewType>;

    std::ranges::sentinel_t<BaseType> end_ = std::ranges::sentinel_t<BaseType>();

    constexpr explicit SentinelType(std::ranges::sentinel_t<BaseType> end) : end_(std::move(end)) { }

    friend EnumerateView;

public:
    SentinelType() = default;

    constexpr SentinelType(SentinelType<!IsConst> other)
    requires IsConst && std::convertible_to<std::ranges::sentinel_t<ViewType>, std::ranges::sentinel_t<BaseType>>
        : end_(std::move(other.end_))
    {
    }

    constexpr std::ranges::sentinel_t<BaseType> base() const { return end_; }

    template <bool OtherConst>
    requires std::sentinel_for<std::ranges::sentinel_t<BaseType>, std::ranges::iterator_t<detail::maybe_const_t<OtherConst, ViewType>>>
    friend constexpr bool operator==(const IteratorType<OtherConst>& lhs, const SentinelType& rhs)
    {
        return lhs.current_ == rhs.end_;
    }

    template <bool OtherConst>
    requires std::
        sized_sentinel_for<std::ranges::sentinel_t<BaseType>, std::ranges::iterator_t<detail::maybe_const_t<OtherConst, ViewType>>>
        friend constexpr std::ranges::range_difference_t<detail::maybe_const_t<OtherConst, ViewType>>
        operator-(const IteratorType<OtherConst>& lhs, const SentinelType& rhs)
    {
        return lhs.current_ - rhs.end_;
    }

    template <bool OtherConst>
    requires std::
        sized_sentinel_for<std::ranges::sentinel_t<BaseType>, std::ranges::iterator_t<detail::maybe_const_t<OtherConst, ViewType>>>
        friend constexpr std::ranges::range_difference_t<detail::maybe_const_t<OtherConst, ViewType>>
        operator-(const SentinelType& lhs, const IteratorType<OtherConst>& rhs)
    {
        return lhs.end_ - rhs.current_;
    }
};

namespace views
{

namespace adaptor
{
/// True if the range adaptor Adaptor can be applied with Args.
template <typename AdaptorType, typename... Args>
concept adaptor_invocable = requires { std::declval<AdaptorType>()(std::declval<Args>()...); };

/// True if the range adaptor non-closure Adaptor can be partially applied
/// with Args.
template <typename Adaptor, typename... Args>
concept adaptor_partial_app_viable
    = (Adaptor::arity_ > 1) && (sizeof...(Args) == Adaptor::arity_ - 1) && (std::constructible_from<std::decay_t<Args>, Args> && ...);

template <typename Adaptor, typename... Args>
struct Partial;

template <typename Lhs, typename Rhs>
struct Pipe;

/// The base class of every range adaptor closure.
///
/// The derived class should define the optional static data member
/// has_simple_call_op_ to true if the behavior of this adaptor is
/// independent of the constness/value category of the adaptor object.
template <typename Derived>
struct RangeAdaptorClosure;

template <typename Tp, typename Up>
requires(!std::same_as<Tp, RangeAdaptorClosure<Up>>)
void is_range_adaptor_closure_fn(const Tp&, const RangeAdaptorClosure<Up>&); /// not defined

template <typename Tp>
concept is_range_adaptor_closure = requires(Tp t) { adaptor::is_range_adaptor_closure_fn(t, t); };

/// range | adaptor is equivalent to adaptor(range).
template <typename Self, typename Range>
requires is_range_adaptor_closure<Self> && adaptor_invocable<Self, Range>
constexpr auto operator|(Range&& r, Self&& self)
{
    return std::forward<Self>(self)(std::forward<Range>(r));
}

/// Compose the adaptors lhs and rhs into a pipeline, returning
/// another range adaptor closure object.
template <typename Lhs, typename Rhs>
requires is_range_adaptor_closure<Lhs> && is_range_adaptor_closure<Rhs>
constexpr auto operator|(Lhs&& lhs, Rhs&& rhs)
{
    return Pipe<std::decay_t<Lhs>, std::decay_t<Rhs>>{std::forward<Lhs>(lhs), std::forward<Rhs>(rhs)};
}

template <typename _Derived>
struct RangeAdaptorClosure
{
    /// In non-modules compilation ADL finds these operators either way and
    /// the friend declarations are redundant.  But with the std module these
    /// friend declarations enable ADL to find these operators without having
    /// to export them.
    template <typename Self, typename Range>
    requires is_range_adaptor_closure<Self> && adaptor_invocable<Self, Range>
    friend constexpr auto operator|(Range&& r, Self&& self);

    template <typename Lhs, typename Rhs>
    requires is_range_adaptor_closure<Lhs> && is_range_adaptor_closure<Rhs>
    friend constexpr auto operator|(Lhs&& lhs, Rhs&& rhs);
};

/// The base class of every range adaptor non-closure.
///
/// The static data member _Derived::arity_ must contain the total number of
/// arguments that the adaptor takes, and the class _Derived must introduce
/// RangeAdaptor::operator() into the class scope via a using-declaration.
///
/// The optional static data member _Derived::has_simple_extra_args_ should
/// be defined to true if the behavior of this adaptor is independent of the
/// constness/value category of the extra arguments.  This data member could
/// also be defined as a variable template parameterized by the types of the
/// extra arguments.
template <typename _Derived>
struct RangeAdaptor
{
    /// Partially apply the arguments args to the range adaptor _Derived,
    /// returning a range adaptor closure object.
    template <typename... Args>
    requires adaptor_partial_app_viable<_Derived, Args...>
    constexpr auto operator()(Args&&... args) const
    {
        return Partial<_Derived, std::decay_t<Args>...>{0, std::forward<Args>(args)...};
    }
};

template <typename Tp, typename _Up>
struct like_impl; /// Tp must be a reference and _Up an lvalue reference

template <typename Tp, typename _Up>
struct like_impl<Tp&, _Up&>
{
    using type = _Up&;
};

template <typename Tp, typename _Up>
struct like_impl<const Tp&, _Up&>
{
    using type = const _Up&;
};

template <typename Tp, typename _Up>
struct like_impl<Tp&&, _Up&>
{
    using type = _Up&&;
};

template <typename Tp, typename _Up>
struct like_impl<const Tp&&, _Up&>
{
    using type = const _Up&&;
};

template <typename Tp, typename _Up>
using like_t = typename like_impl<Tp&&, _Up&>::type;
/// True if the range adaptor closure Adaptor has a simple operator(), i.e.
/// one that's not overloaded according to constness or value category of the
/// Adaptor object.
template <typename Adaptor>
concept closure_has_simple_call_op = Adaptor::has_simple_call_o_p;

/// True if the behavior of the range adaptor non-closure Adaptor is
/// independent of the value category of its extra arguments Args.
template <typename Adaptor, typename... Args>
concept adaptor_has_simple_extra_args = Adaptor::has_simple_extra_args_ || Adaptor::template has_simple_extra_arg_s<Args...>;

/// A range adaptor closure that represents partial application of
/// the range adaptor Adaptor with arguments Args.
template <typename Adaptor, typename... Args>
struct Partial : RangeAdaptorClosure<Partial<Adaptor, Args...>>
{
    std::tuple<Args...> args_;

    /// First parameter is to ensure this constructor is never used
    /// instead of the copy/move constructor.
    template <typename... Ts>
    constexpr Partial(int, Ts&&... args) : args_(std::forward<Ts>(args)...)
    {
    }

    /// Invoke Adaptor with arguments r, args_... according to the
    /// value category of this _Partial object.
    template <typename Self, typename Range>
    requires adaptor_invocable<Adaptor, Range, like_t<Self, Args>...>
    constexpr auto operator()(this Self&& self, Range&& r)
    {
        auto forwarder = [&r](auto&&... args) { return Adaptor{}(std::forward<Range>(r), std::forward<decltype(args)>(args)...); };
        return std::apply(forwarder, like_t<Self, Partial>(self).args_);
    }
};

/// A lightweight specialization of the above primary template for
/// the common case where Adaptor accepts a single extra argument.
template <typename Adaptor, typename Arg>
struct Partial<Adaptor, Arg> : RangeAdaptorClosure<Partial<Adaptor, Arg>>
{
    Arg arg_;

    template <typename Tp>
    constexpr Partial(int, Tp&& arg) : arg_(std::forward<Tp>(arg))
    {
    }

    template <typename Self, typename Range>
    requires adaptor_invocable<Adaptor, Range, like_t<Self, Arg>>
    constexpr auto operator()(this Self&& self, Range&& r)
    {
        return Adaptor{}(std::forward<Range>(r), like_t<Self, Partial>(self).arg_);
    }
};

/// Partial specialization of the primary template for the case where the extra
/// arguments of the adaptor can always be safely and efficiently forwarded by
/// const reference.  This lets us get away with a single operator() overload,
/// which makes overload resolution failure diagnostics more concise.
template <typename Adaptor, typename... Args>
requires adaptor_has_simple_extra_args<Adaptor, Args...> && (std::is_trivially_copyable_v<Args> && ...)
struct Partial<Adaptor, Args...> : RangeAdaptorClosure<Partial<Adaptor, Args...>>
{
    std::tuple<Args...> args_;

    template <typename... Ts>
    constexpr Partial(int, Ts&&... args) : args_(std::forward<Ts>(args)...)
    {
    }

    /// Invoke Adaptor with arguments r, const args_&... regardless
    /// of the value category of this _Partial object.
    template <typename Range>
    requires adaptor_invocable<Adaptor, Range, const Args&...>
    constexpr auto operator()(Range&& r) const
    {
        auto forwarder = [&r](const auto&... args) { return Adaptor{}(std::forward<Range>(r), args...); };
        return std::apply(forwarder, args_);
    }

    static constexpr bool has_simple_call_op_ = true;
};

/// A lightweight specialization of the above template for the common case
/// where Adaptor accepts a single extra argument.
template <typename Adaptor, typename Arg>
requires adaptor_has_simple_extra_args<Adaptor, Arg> && std::is_trivially_copyable_v<Arg>
struct Partial<Adaptor, Arg> : RangeAdaptorClosure<Partial<Adaptor, Arg>>
{
    Arg arg_;

    template <typename Tp>
    constexpr Partial(int, Tp&& arg) : arg_(std::forward<Tp>(arg))
    {
    }

    template <typename Range>
    requires adaptor_invocable<Adaptor, Range, const Arg&>
    constexpr auto operator()(Range&& r) const
    {
        return Adaptor{}(std::forward<Range>(r), arg_);
    }

    static constexpr bool has_simple_call_op_ = true;
};

template <typename Lhs, typename Rhs, typename Range>
concept pipe_invocable = requires { std::declval<Rhs>()(std::declval<Lhs>()(std::declval<Range>())); };


/// A range adaptor closure that represents composition of the range
/// adaptor closures Lhs and Rhs.
template <typename Lhs, typename Rhs>
struct Pipe : RangeAdaptorClosure<Pipe<Lhs, Rhs>>
{
    [[no_unique_address]] Lhs lhs_;
    [[no_unique_address]] Rhs rhs_;

    template <typename Tp, typename _Up>
    constexpr Pipe(Tp&& lhs, _Up&& rhs) : lhs_(std::forward<Tp>(lhs)), rhs_(std::forward<_Up>(rhs))
    {
    }

    /// Invoke rhs_(lhs_(r)) according to the value category of this
    /// range adaptor closure object.
    template <typename Self, typename Range>
    requires pipe_invocable<like_t<Self, Lhs>, like_t<Self, Rhs>, Range>
    constexpr auto operator()(this Self&& self, Range&& r)
    {
        return (like_t<Self, Pipe>(self).rhs_(like_t<Self, Pipe>(self).lhs_(std::forward<Range>(r))));
    }
};

/// A partial specialization of the above primary template for the case where
/// both adaptor operands have a simple operator().  This in turn lets us
/// implement composition using a single simple operator(), which makes
/// overload resolution failure diagnostics more concise.
template <typename Lhs, typename Rhs>
requires closure_has_simple_call_op<Lhs> && closure_has_simple_call_op<Rhs>
struct Pipe<Lhs, Rhs> : RangeAdaptorClosure<Pipe<Lhs, Rhs>>
{
    [[no_unique_address]] Lhs lhs_;
    [[no_unique_address]] Rhs rhs_;

    template <typename Tp, typename _Up>
    constexpr Pipe(Tp&& lhs, _Up&& rhs) : lhs_(std::forward<Tp>(lhs)), rhs_(std::forward<_Up>(rhs))
    {
    }

    template <typename Range>
    requires pipe_invocable<const Lhs&, const Rhs&, Range>
    constexpr auto operator()(Range&& r) const
    {
        return rhs_(lhs_(std::forward<Range>(r)));
    }

    static constexpr bool has_simple_call_op_ = true;
};
}

namespace detail
{
template <typename T>
concept can_enumerate_view = requires { EnumerateView<std::views::all_t<T>>(std::declval<T>()); };
}

struct Enumerate : adaptor::RangeAdaptorClosure<Enumerate>
{
    template <std::ranges::viewable_range Range>
    requires detail::can_enumerate_view<Range>
    constexpr auto operator() [[nodiscard]] (Range&& range) const
    {
        return EnumerateView<std::views::all_t<Range>>(std::forward<Range>(range));
    }
};

inline constexpr Enumerate enumerate_old;
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
#else
namespace ranges
{

template <class Container, std::ranges::input_range RangeType, class... Args>
constexpr Container to(RangeType&& range, Args&&... args)
{
    return std::ranges::to<Container>(std::forward<RangeType>(range), std::forward<Args>(args)...);
}

template <template <class...> class Container, std::ranges::input_range RangeType, class... Args>
constexpr auto to(RangeType&& range, Args&&... args)
{
    using DeducerExpre = typename std::ranges::_Deducer<Container, RangeType, Args...>::type;
    return ranges::to<DeducerExpre>(std::forward<RangeType>(range), std::forward<Args>(args)...);
}

template <class Container, class... Args>
constexpr auto to(Args&&... args)
{
    return std::ranges::to<Container>(std::forward<Args>(args)...);
}

template <template <class...> class Container, class... Args>
constexpr auto to(Args&&... args)
{
    return std::ranges::to<Container>(std::forward<Args>(args)...);
}

}
#endif
}

template <typename T>
inline constexpr bool std::ranges::enable_borrowed_range<NES::EnumerateView<T>> = std::ranges::enable_borrowed_range<T>;*/

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
