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
#include <cstddef>
#include <cstdint>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Identifiers/NESStrongType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Strings.hpp>
#include <SystestState.hpp>


template <typename T, typename Tag>
class ResultCheckStrongType
{
public:
    explicit constexpr ResultCheckStrongType(T v) : v(v) { }
    using Underlying = T;
    using TypeTag = Tag;
    friend std::ostream& operator<<(std::ostream& os, const ResultCheckStrongType& strongType) { return os << strongType.getRawValue(); }
    [[nodiscard]] const T& getRawValue() const { return v; }
    [[nodiscard]] T& getRawValue() { return v; }

private:
    T v;
};

using ExpectedResultField = ResultCheckStrongType<std::string, struct ExpectedResultFields_>;
using ActualResultField = ResultCheckStrongType<std::string, struct ActualResultFields_>;

template <typename FieldType, typename Tag>
class ResultTuple
{
public:
    explicit ResultTuple(std::string tuple) : tuple(std::move(tuple)) { }
    using TupleType = Tag;

    [[nodiscard]] size_t size() const { return tuple.size(); }
    friend std::ostream& operator<<(std::ostream& os, const ResultTuple& resultTuple) { return os << resultTuple.tuple; }
    [[nodiscard]] const std::string& getRawValue() const { return tuple; }

    [[nodiscard]] std::vector<FieldType> getFields() const
    {
        auto result = tuple | std::views::split(' ')
            | std::views::transform([](auto&& range) { return FieldType(std::string(range.begin(), range.end())); })
            | std::ranges::to<std::vector>();
        return result;
    }

private:
    std::string tuple;
};


inline void sortOnField(std::vector<std::string>& results, const size_t fieldIdx)
{
    std::ranges::sort(
        results,
        [fieldIdx](const std::string& lhs, const std::string& rhs)
        {
            const auto lhsField = std::string_view((lhs | std::views::split(' ') | std::views::drop(fieldIdx)).front());
            const auto rhsField = std::string_view((rhs | std::views::split(' ') | std::views::drop(fieldIdx)).front());
            return lhsField < rhsField;
        });
};

template <typename TupleIdxType, typename Tag>
class ResultTuples
{
public:
    explicit ResultTuples(std::vector<std::string> results, const size_t expectedResultsFieldSortIdx) : results(std::move(results))
    {
        /// We allow commas in the result and the expected result. To ensure they are equal we remove them from both.
        /// Additionally, we remove double spaces, as we expect a single space between the fields
        std::ranges::for_each(this->results, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
        std::ranges::for_each(this->results, NES::Util::removeDoubleSpaces);

        sortOnField(this->results, expectedResultsFieldSortIdx);
    }
    ~ResultTuples() = default;
    using TupleType = Tag;

    [[nodiscard]] TupleType getTuple(const TupleIdxType tupleIdx) const { return TupleType(results.at(tupleIdx.getRawValue())); }
    [[nodiscard]] size_t size() const { return results.size(); }

private:
    std::vector<std::string> results;
};

template <typename Tag>
class ErrorStream
{
public:
    explicit ErrorStream() = default;
    ~ErrorStream() = default;
    explicit ErrorStream(std::stringstream errorStream) : errorStream(std::move(errorStream)) { }

    using ErrorStreamType = Tag;

    ErrorStream(const ErrorStream&) = delete;
    ErrorStream& operator=(const ErrorStream&) = delete;
    ErrorStream(ErrorStream&&) = default;
    ErrorStream& operator=(ErrorStream&&) = default;

    bool hasMismatch() const { return not errorStream.view().empty(); }

    friend std::ostream& operator<<(std::ostream& os, const ErrorStream& ses) { return os << ses.errorStream.str(); }
    template <typename T>
    ErrorStream& operator<<(T&& value)
    {
        errorStream << std::forward<T>(value);
        return *this;
    }

private:
    std::stringstream errorStream;
};

using ExpectedResultIndex = NES::NESStrongType<uint64_t, struct ExpectedResultIndex_, 0, 1>;
using ActualResultIndex = NES::NESStrongType<uint64_t, struct ActualResultIndex_, 0, 1>;
using ExpectedResultTuple = ResultTuple<ExpectedResultField, struct ExpectedResultTuple_>;
using ActualResultTuple = ResultTuple<ActualResultField, struct ActualResultTuple_>;
using ExpectedResultTuples = ResultTuples<ExpectedResultIndex, ExpectedResultTuple>;
using ActualResultTuples = ResultTuples<ActualResultIndex, ActualResultTuple>;
using ExpectedResultSchema = ResultCheckStrongType<NES::Systest::SystestSchema, struct ExpectedResultSchema_>;
using ActualResultSchema = ResultCheckStrongType<NES::Systest::SystestSchema, struct ActualResultSchema_>;
using SchemaErrorStream = ErrorStream<struct SchemaErrorStream_>;
using ResultErrorStream = ErrorStream<struct ResultErrorStream_>;

FMT_OSTREAM(::SchemaErrorStream);
FMT_OSTREAM(::ResultErrorStream);
FMT_OSTREAM(::ExpectedResultTuple);
FMT_OSTREAM(::ActualResultTuple);
FMT_OSTREAM(::ActualResultField);
FMT_OSTREAM(::ExpectedResultField);