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
#include <ostream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <RawValueParser.hpp>

namespace NES
{

/// Restricts the IndexerMetaData that an InputFormatIndexer receives from the InputFormatter
template <typename T>
concept IndexerMetaDataType = requires(
    ParserConfig config,
    const Nautilus::Interface::BufferRef::TupleBufferRef& tupleBufferRef,
    T indexerMetaData,
    std::ostream& spanningTuple) {
    T(config, tupleBufferRef);
    /// Assumes a fixed set of symbols that separate tuples
    /// InputFormatIndexers without tuple delimiters should return an empty string
    { indexerMetaData.getTupleDelimitingBytes() } -> std::same_as<std::string_view>;
    { indexerMetaData.getQuotationType() } -> std::same_as<QuotationType>;
};

template <typename T>
concept FieldIndexFunctionType = requires(const T& indexFunction) {
    { indexFunction.getByteOffsetOfFirstTuple() };
    { indexFunction.getByteOffsetOfLastTuple() };
    { indexFunction.getTotalNumberOfTuples() };
};


class RawTupleBuffer;
template <typename T>
concept InputFormatIndexerType = IndexerMetaDataType<typename T::IndexerMetaData>
    && FieldIndexFunctionType<typename T::FieldIndexFunctionType> && requires(const T& indexer) {
           {
               indexer.indexRawBuffer(
                   std::declval<typename T::FieldIndexFunctionType&>(),
                   std::declval<RawTupleBuffer&>(),
                   std::declval<typename T::IndexerMetaData&>())
           };
           { std::declval<std::ostream&>() << indexer } -> std::same_as<std::ostream&>;
       };
}
