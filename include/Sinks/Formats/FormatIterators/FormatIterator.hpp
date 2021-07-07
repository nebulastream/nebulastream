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

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Sinks/Formats/FormatType.hpp>
#include <utility>

namespace NES {

/**
 * @brief this class is used for iterating over a single buffer and extracting out the tuples
 */
class FormatIterator {
  public:
    explicit FormatIterator(SchemaPtr schema, Runtime::TupleBuffer buffer, FormatTypes sinkFormatType);

    class Iterator {
        friend class FormatIterator;

      public:
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;

        explicit Iterator(uint64_t currentSeek, Runtime::TupleBuffer buffer, const SchemaPtr& schema, FormatTypes sinkFormatType)
            : buffer(std::move(buffer)), sinkFormatType(sinkFormatType) {
            auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

            // Iterate over all fields of a tuple. Store sizes in fieldOffsets, to calculate correct offsets in the step below.
            // Also, store types of fields in a separate array. Is later used to convert values to strings correctly.
            // Iteratively add up all the sizes in the offset array, to correctly determine where each field starts in the TupleBuffer
            uint32_t fieldOffset = 0;
            for (const auto& field : schema->fields) {
                auto physicalType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
                fieldTypes.push_back(physicalType);
                fieldNames.push_back(field->getName());
                fieldOffsets.push_back(fieldOffset);
                fieldOffset += physicalType->size();
            }
            tupleOffset = fieldOffset;
            this->currentSeek = currentSeek * fieldOffset;
        };

        /**
         * @brief Increases the current bufferIndex by one
         * @return iterator
         */
        Iterator& operator++() {
            currentSeek = currentSeek + tupleOffset;
            return *this;
        };

        /**
         * @brief Accesses the TupleBuffer at the current bufferIndex and returns the address at that index
         * @return
         */
        std::string operator*() {
            switch (sinkFormatType) {
                case FormatTypes::JSON_FORMAT: return dataJson();
                case FormatTypes::TEXT_FORMAT:
                case FormatTypes::NES_FORMAT:
                case FormatTypes::CSV_FORMAT:
                default: NES_NOT_IMPLEMENTED();
            }
        };

        friend bool operator==(const Iterator& a, const Iterator& b) { return a.currentSeek == b.currentSeek; };
        friend bool operator!=(const Iterator& a, const Iterator& b) { return a.currentSeek != b.currentSeek; };

      private:
        /**
         * @brief return current tuple in json format
         * @return string in json representation
         */
        std::string dataJson() {
            uint8_t* tuplePointer = &this->buffer.getBuffer<uint8_t>()[currentSeek];

            // Iterate over all fields in a tuple. Get field offsets from fieldOffsets array. Use fieldNames as keys and TupleBuffer
            // values as the corresponding values
            // Adding the first tuple before the loop avoids checking if last tuple is processed in order to omit "," after json value
            std::stringstream jsonMessage;
            jsonMessage << "{";
            for (uint32_t currentField = 0; currentField < fieldNames.size(); currentField++) {
                auto currentFieldOffset = fieldOffsets[currentField];
                auto currentFieldType = fieldTypes[currentField];
                auto fieldName = fieldNames[currentField];
                auto fieldValue = currentFieldType->convertRawToString(&tuplePointer[currentFieldOffset]);
                jsonMessage << R"(")" + fieldName + R"(":)" + R"(")" + fieldValue + R"(")";
                if (currentField != fieldNames.size() - 1) {
                    jsonMessage << ",";
                }
            }
            jsonMessage << "}";
            return jsonMessage.str();
        }

        /**
         * current seek position in the buffer
         */
        uint64_t currentSeek;

        /**
         * @brief Buffer to be read
         */
        Runtime::TupleBuffer buffer;

        /**
         * @brief Format in which data is to be read
         */
        FormatTypes sinkFormatType;

        /**
         * @brief Offset between two consecutive tuples
         */
        uint64_t tupleOffset;

        /**
         * @brief Vector containing the offset for each field
         * From field with Size = {64, 32, 32, 128} corresponding fieldOffsets = {64, 96, 128, 256} (actual field offsets)
         */
        std::vector<uint32_t> fieldOffsets;

        /**
         * @brief Physical types of the fields
         */
        std::vector<PhysicalTypePtr> fieldTypes;

        /**
         * @brief Name of the fields
         */
        std::vector<std::string> fieldNames;
    };

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
#endif//NES_INCLUDE_NODEENGINE_FORMATITERATORS_SINKFORMATITERATOR_HPP_
