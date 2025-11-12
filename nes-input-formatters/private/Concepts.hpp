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
#include <cstddef>
#include <ostream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{

/// Restricts the IndexerMetaData that an InputFormatIndexer receives from the InputFormatterTask
template <typename T>
concept IndexerMetaDataType = requires(ParserConfig config, Schema schema, T indexerMetaData, std::ostream& spanningTuple) {
    T(config, schema);
    /// Assumes a fixed set of symbols that separate tuples
    /// InputFormatIndexers without tuple delimiters should return an empty string
    { indexerMetaData.getTupleDelimitingBytes() } -> std::same_as<std::string_view>;
};

template <typename T>
concept FieldIndexFunctionType = requires(const T& indexFunction) {
    { indexFunction.getOffsetOfFirstTupleDelimiter() };
    { indexFunction.getOffsetOfLastTupleDelimiter() };
    { indexFunction.getTotalNumberOfTuples() };
    {
        indexFunction.readFieldAt(std::declval<std::string_view>(), std::declval<size_t>(), std::declval<size_t>())
    } -> std::same_as<std::string_view>;
};

template <typename T>
concept InputFormatIndexerType
    = IndexerMetaDataType<typename T::IndexerMetaData> && std::same_as<std::remove_cv_t<decltype(T::IsFormattingRequired)>, bool>
    && std::same_as<std::remove_cv_t<decltype(T::HasSpanningTuple)>, bool> && FieldIndexFunctionType<typename T::FieldIndexFunctionType>
    && requires(const T& indexer) {
           {
               indexer.indexRawBuffer(
                   std::declval<typename T::FieldIndexFunctionType&>(),
                   std::declval<RawTupleBuffer&>(),
                   std::declval<typename T::IndexerMetaData&>())
           };
           { std::declval<std::ostream&>() << indexer } -> std::same_as<std::ostream&>;
       };
}
