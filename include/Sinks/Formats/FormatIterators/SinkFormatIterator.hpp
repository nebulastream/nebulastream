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

#ifndef NES_INCLUDE_NODEENGINE_FORMATITERATORS_SINKFORMATITERATOR_HPP_
#define NES_INCLUDE_NODEENGINE_FORMATITERATORS_SINKFORMATITERATOR_HPP_

/**
 * @brief this class covers the different iterators for each format that NES supports
 */
namespace NES {

enum SinkFormatIteratorTypes { CSV_FORMAT_ITERATOR, JSON_FORMAT_ITERATOR, NES_FORMAT_ITERATOR, TEXT_FORMAT_ITERATOR };

class SinkFormatIterator;
typedef std::shared_ptr<SinkFormatIterator> SinkFormatIteratorPtr;

class SinkFormatIterator {
  public:
    explicit SinkFormatIterator(SchemaPtr schema, NodeEngine::TupleBufferPtr);
    explicit SinkFormatIterator();

    class iterator;
    typedef std::shared_ptr<iterator> iteratorPtr;
    class iterator : public std::enable_shared_from_this<iterator>,
                     std::iterator<std::forward_iterator_tag, NodeEngine::TupleBufferPtr, NodeEngine::TupleBufferPtr,
                                                              NodeEngine::TupleBuffer*, NodeEngine::TupleBufferPtr&> {
        friend class SinkFormatIterator;

      public:
        /**
         * @brief Increases the current bufferIndex by one
         * @return iterator
         */
        virtual void operator++();

        /**
         * @brief Checks if the iterators are not at the same position
         */
        virtual bool operator!=(const std::shared_ptr<SinkFormatIterator::iterator> other) const;

        /**
         * @brief Accesses the TupleBuffer at the current bufferIndex and returns the address at that index
         * @return
         */
        virtual uint8_t operator*();

        /**
        * @brief Checks if IteratorType is of type SinkFormatIterator
        * @tparam IteratorType
        * @return bool true if IteratorType is of type SinkFormatIterator
        */
        template<class IteratorType>
        bool instanceOf() {
            if (dynamic_cast<IteratorType*>(this)) {
                return true;
            };
            return false;
        };

        /**
        * @brief Dynamically casts the SinkFormatIterator to IteratorType
        * @tparam IteratorType
        * @return returns a shared pointer of IteratorType
        */
        template<class IteratorType>
        std::shared_ptr<IteratorType> as() {
            NES_DEBUG("Trying iterator cast");
            if (instanceOf<IteratorType>()) {
                return std::dynamic_pointer_cast<IteratorType>(this->shared_from_this());
            } else {
                NES_FATAL_ERROR("We performed an invalid cast");
                throw std::bad_cast();
            }
        }

      private:
        explicit iterator(NodeEngine::TupleBufferPtr buffer, SchemaPtr schema, uint64_t index);
        NodeEngine::TupleBufferPtr bufferPtr;
        SchemaPtr schemaPtr;
        uint64_t bufferIndex;

      protected:
        explicit iterator();
    };

    /**
     * @brief Starts a new iterator with a bufferIndex of 0
     * @return iteratorPtr
     */
    virtual iteratorPtr begin();

    /**
    * @brief The end of this iterator has a bufferIndex that is equal to the number of tuples in the TupleBuffer
    * @return iteratorPtr
    */
    virtual iteratorPtr end();

  private:
    SchemaPtr schema;
    NodeEngine::TupleBufferPtr buffer;
};
}// namespace NES
#endif//NES_INCLUDE_NODEENGINE_FORMATITERATORS_SINKFORMATITERATOR_HPP_
