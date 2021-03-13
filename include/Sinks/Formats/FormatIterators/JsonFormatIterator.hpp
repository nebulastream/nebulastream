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

#ifndef NES_INCLUDE_NODEENGINE_FORMATITERATORS_JSONFORMATITERATOR_HPP_
#define NES_INCLUDE_NODEENGINE_FORMATITERATORS_JSONFORMATITERATOR_HPP_

#include <Sinks/Formats/FormatIterators/SinkFormatIterator.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
namespace NES {

class JsonFormatIterator : public SinkFormatIterator {
  public:
    explicit JsonFormatIterator(SchemaPtr schema, NodeEngine::TupleBufferPtr);

    class jsonIterator;
    typedef std::shared_ptr<jsonIterator> jsonIteratorPtr;
    class jsonIterator : public SinkFormatIterator::iterator {
        friend class JsonFormatIterator;

      public:
        /**
         * @brief Increases the current bufferIndex by one
         * @return iterator
         */
        void operator++();

        /**
         * @brief Checks if the iterators are not at the same position
         */
        bool operator!=(const JsonFormatIterator::jsonIteratorPtr other) const;

        /**
         * @brief Returns JSON string at given iterator position
         * @return
         */
        std::string getCurrentTupleAsJson();

        typedef std::shared_ptr<iterator> iteratorPtr;

      private:
        explicit jsonIterator(NodeEngine::TupleBufferPtr buffer, SchemaPtr schema, uint64_t index);
        NodeEngine::TupleBufferPtr bufferPtr;
        SchemaPtr schemaPtr;
        uint64_t bufferIndex;
        std::vector<uint32_t> fieldOffsets;
        std::vector<PhysicalTypePtr> types;
    };

    /**
     * @brief Starts a new iterator with a bufferIndex of 0
     * @return iteratorPtr
     */
    SinkFormatIterator::iteratorPtr begin();

    /**
    * @brief The end of this iterator has a bufferIndex that is equal to the number of tuples in the TupleBuffer
    * @return iteratorPtr
    */
    SinkFormatIterator::iteratorPtr end();

    /**
     * @brief return sink format
     * @return sink format
     */
    SinkFormatIteratorTypes getSinkFormat();

  private:
    SchemaPtr schema;
    NodeEngine::TupleBufferPtr buffer;
};
}

#endif//NES_INCLUDE_NODEENGINE_FORMATITERATORS_JSONFORMATITERATOR_HPP_
