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

#include <algorithm>
#include <cstdint>
#include <new>
#include <string>
#include <vector>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/function.hpp>
#include <nautilus/nautilus_function.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::AggregationDetail
{

/// Short stable mnemonic per numeric type, used to mint the NautilusFunction symbol name.
template <typename T>
constexpr const char* medianTypeMnemonic();
template <>
constexpr const char* medianTypeMnemonic<int8_t>()
{
    return "i8";
}
template <>
constexpr const char* medianTypeMnemonic<int16_t>()
{
    return "i16";
}
template <>
constexpr const char* medianTypeMnemonic<int32_t>()
{
    return "i32";
}
template <>
constexpr const char* medianTypeMnemonic<int64_t>()
{
    return "i64";
}
template <>
constexpr const char* medianTypeMnemonic<uint8_t>()
{
    return "u8";
}
template <>
constexpr const char* medianTypeMnemonic<uint16_t>()
{
    return "u16";
}
template <>
constexpr const char* medianTypeMnemonic<uint32_t>()
{
    return "u32";
}
template <>
constexpr const char* medianTypeMnemonic<uint64_t>()
{
    return "u64";
}
template <>
constexpr const char* medianTypeMnemonic<float>()
{
    return "f32";
}
template <>
constexpr const char* medianTypeMnemonic<double>()
{
    return "f64";
}

template <typename T, bool Nullable>
inline std::string medianName(const char* op)
{
    std::string s = "median_";
    s += op;
    s += '_';
    s += medianTypeMnemonic<T>();
    s += Nullable ? "_nullable" : "_nonnull";
    return s;
}

/// C++ runtime helpers, called via `nautilus::invoke` from the traced primitives below.
/// These are plain C++ — the C++ compiler (not the JIT) handles them. Each (T) instantiation
/// produces one runtime function compiled once and called from N call sites.

/// Append a single typed value to the PagedVector, allocating a new page from the buffer pool
/// when the current page is full. The bufferSize is fetched from the provider so the traced
/// caller stays a single `nautilus::invoke` with only (state, value, provider) args.
template <typename T>
void appendValueToPagedVector(PagedVector* pv, T value, AbstractBufferProvider* bufferProvider)
{
    const uint64_t bufferSize = bufferProvider->getBufferSize();
    const uint64_t capacity = bufferSize / sizeof(T);
    pv->appendPageIfFull(bufferProvider, capacity, bufferSize);
    auto& page = pv->getLastPageMutable();
    auto memArea = page.getAvailableMemoryArea<T>();
    memArea[page.getNumberOfTuples()] = value;
    page.setNumberOfTuples(page.getNumberOfTuples() + 1);
}

/// Concatenate the pages of `src` into `dst`. PagedVector::copyFrom handles the page-list bookkeeping
/// (cumulative sums) so this stays a single CALL in the trace.
inline void mergePagedVectors(PagedVector* dst, const PagedVector* src)
{
    dst->copyFrom(*src);
}

/// Compute the median of all T values stored in the PagedVector.
/// Type-dependent only in T (the comparator and operator+); for a given T this is compiled once by the
/// C++ compiler. The Nautilus trace sees a single `Call` op per Median's `lower` call site.
template <typename T>
double computeMedianFromPagedVector(const PagedVector* pv)
{
    const auto total = pv->getTotalNumberOfEntries();
    if (total == 0)
    {
        return 0.0;
    }
    std::vector<T> values;
    values.reserve(total);
    const auto numPages = pv->getNumberOfPages();
    for (uint64_t i = 0; i < numPages; ++i)
    {
        const auto& page = pv->getPage(i);
        const auto n = page.getNumberOfTuples();
        const auto memArea = page.template getAvailableMemoryArea<const T>();
        for (uint64_t j = 0; j < n; ++j)
        {
            values.push_back(memArea[j]);
        }
    }
    const size_t mid = values.size() / 2;
    std::nth_element(values.begin(), values.begin() + mid, values.end());
    if (values.size() % 2 == 1)
    {
        return static_cast<double>(values[mid]);
    }
    std::nth_element(values.begin(), values.begin() + mid - 1, values.begin() + mid);
    return (static_cast<double>(values[mid - 1]) + static_cast<double>(values[mid])) / 2.0;
}

inline void placementNewPagedVector(void* p)
{
    new (p) PagedVector();
}

inline void destroyPagedVector(PagedVector* p)
{
    p->~PagedVector();
}

/// Traced primitives + NautilusFunction wrappers. Each (T, Nullable) op compiles its traced body once
/// into the JIT module; call sites emit a single `Call` op against the wrapper.
///
/// State layout:
///   Nullable = false  →  [ PagedVector                                  ]
///   Nullable = true   →  [ uint64_t isNull (8B padded) ][ PagedVector  ]
/// The 8-byte head padding preserves PagedVector's alignment.
template <typename T, bool Nullable>
struct TypedMedian;

template <typename T>
struct TypedMedian<T, false>
{
    static constexpr uint64_t vectorOffset = 0;

    static void liftImpl(
        nautilus::val<int8_t*> state,
        nautilus::val<T> value,
        nautilus::val<AbstractBufferProvider*> bufferProvider)
    {
        auto pvPtr = static_cast<nautilus::val<PagedVector*>>(state);
        nautilus::invoke(appendValueToPagedVector<T>, pvPtr, value, bufferProvider);
    }

    static void combineImpl(nautilus::val<int8_t*> lhs, nautilus::val<int8_t*> rhs)
    {
        auto lhsPv = static_cast<nautilus::val<PagedVector*>>(lhs);
        auto rhsPv = static_cast<nautilus::val<PagedVector*>>(rhs);
        nautilus::invoke(mergePagedVectors, lhsPv, rhsPv);
    }

    static nautilus::val<double> lowerImpl(nautilus::val<int8_t*> state)
    {
        auto pvPtr = static_cast<nautilus::val<PagedVector*>>(state);
        return nautilus::invoke(computeMedianFromPagedVector<T>, pvPtr);
    }

    static void resetImpl(nautilus::val<int8_t*> state)
    {
        auto raw = static_cast<nautilus::val<void*>>(state);
        nautilus::invoke(placementNewPagedVector, raw);
    }

    static void cleanupImpl(nautilus::val<int8_t*> state)
    {
        auto pvPtr = static_cast<nautilus::val<PagedVector*>>(state);
        nautilus::invoke(destroyPagedVector, pvPtr);
    }

    static inline nautilus::NautilusFunction lift{medianName<T, false>("lift"), liftImpl};
    static inline nautilus::NautilusFunction combine{medianName<T, false>("combine"), combineImpl};
    static inline nautilus::NautilusFunction lower{medianName<T, false>("lower"), lowerImpl};
    static inline nautilus::NautilusFunction reset{medianName<T, false>("reset"), resetImpl};
    static inline nautilus::NautilusFunction cleanup{medianName<T, false>("cleanup"), cleanupImpl};
};

template <typename T>
struct TypedMedian<T, true>
{
    /// 8 bytes for the padded null head, then the PagedVector. The head is read/written as a plain
    /// `bool` at offset 0; the 7 bytes of padding are unused and preserve PagedVector alignment.
    static constexpr uint64_t vectorOffset = 8;

    static void liftImpl(
        nautilus::val<int8_t*> state,
        nautilus::val<T> value,
        nautilus::val<bool> valueIsNull,
        nautilus::val<AbstractBufferProvider*> bufferProvider)
    {
        auto isNullPtr = static_cast<nautilus::val<bool*>>(state);
        const nautilus::val<bool> oldIsNull = *isNullPtr;
        *isNullPtr = oldIsNull || valueIsNull;
        auto pvPtr = static_cast<nautilus::val<PagedVector*>>(state + nautilus::val<uint64_t>{vectorOffset});
        nautilus::invoke(appendValueToPagedVector<T>, pvPtr, value, bufferProvider);
    }

    static void combineImpl(nautilus::val<int8_t*> lhs, nautilus::val<int8_t*> rhs)
    {
        auto lhsIsNullPtr = static_cast<nautilus::val<bool*>>(lhs);
        auto rhsIsNullPtr = static_cast<nautilus::val<bool*>>(rhs);
        const nautilus::val<bool> lhsIsNull = *lhsIsNullPtr;
        const nautilus::val<bool> rhsIsNull = *rhsIsNullPtr;
        *lhsIsNullPtr = lhsIsNull || rhsIsNull;
        auto lhsPv = static_cast<nautilus::val<PagedVector*>>(lhs + nautilus::val<uint64_t>{vectorOffset});
        auto rhsPv = static_cast<nautilus::val<PagedVector*>>(rhs + nautilus::val<uint64_t>{vectorOffset});
        nautilus::invoke(mergePagedVectors, lhsPv, rhsPv);
    }

    static nautilus::val<double> lowerImpl(nautilus::val<int8_t*> state)
    {
        auto pvPtr = static_cast<nautilus::val<PagedVector*>>(state + nautilus::val<uint64_t>{vectorOffset});
        return nautilus::invoke(computeMedianFromPagedVector<T>, pvPtr);
    }

    static nautilus::val<bool> lowerIsNullImpl(nautilus::val<int8_t*> state)
    {
        auto isNullPtr = static_cast<nautilus::val<bool*>>(state);
        return *isNullPtr;
    }

    static void resetImpl(nautilus::val<int8_t*> state)
    {
        auto isNullPtr = static_cast<nautilus::val<bool*>>(state);
        *isNullPtr = nautilus::val<bool>{false};
        auto raw = static_cast<nautilus::val<void*>>(state + nautilus::val<uint64_t>{vectorOffset});
        nautilus::invoke(placementNewPagedVector, raw);
    }

    static void cleanupImpl(nautilus::val<int8_t*> state)
    {
        auto pvPtr = static_cast<nautilus::val<PagedVector*>>(state + nautilus::val<uint64_t>{vectorOffset});
        nautilus::invoke(destroyPagedVector, pvPtr);
    }

    static inline nautilus::NautilusFunction lift{medianName<T, true>("lift"), liftImpl};
    static inline nautilus::NautilusFunction combine{medianName<T, true>("combine"), combineImpl};
    static inline nautilus::NautilusFunction lower{medianName<T, true>("lower"), lowerImpl};
    static inline nautilus::NautilusFunction lowerIsNull{medianName<T, true>("lower_isnull"), lowerIsNullImpl};
    static inline nautilus::NautilusFunction reset{medianName<T, true>("reset"), resetImpl};
    static inline nautilus::NautilusFunction cleanup{medianName<T, true>("cleanup"), cleanupImpl};
};

}
