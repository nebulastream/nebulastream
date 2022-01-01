/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_SINKS_FORMATS_FORMAT_ITERATORS_FORMAT_ITERATOR_HPP_
#define NES_INCLUDE_SINKS_FORMATS_FORMAT_ITERATORS_FORMAT_ITERATOR_HPP_
#include <Sinks/Formats/FormatIterators/Iterator.hpp>
#include <utility>

namespace NES {

/**
 * @brief this class is used for iterating over a single buffer and extracting out the tuples
 */
class FormatIterator {
  public:
    explicit FormatIterator(SchemaPtr schema, Runtime::TupleBuffer buffer, FormatTypes sinkFormatType);

    /**
     * @brief Starts a new iterator with a bufferIndex of 0
     * @return iteratorPtr
     */
    Iterator begin() { return Iterator(0, buffer, schema, sinkFormatType); };

    /**
    * @brief The end of this iterator has a bufferIndex that is equal to the number of tuples in the TupleBuffer
    * @return iteratorPtr
    */
    Iterator end() { return Iterator(buffer.getNumberOfTuples(), buffer, schema, sinkFormatType); };

  private:
    SchemaPtr schema;
    Runtime::TupleBuffer buffer;
    FormatTypes sinkFormatType;
};
}// namespace NES
#endif// NES_INCLUDE_SINKS_FORMATS_FORMAT_ITERATORS_FORMAT_ITERATOR_HPP_
