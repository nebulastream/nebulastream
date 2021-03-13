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


#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/FormatIterators/JsonFormatIterator.hpp>

namespace NES {

JsonFormatIterator::JsonFormatIterator(SchemaPtr schema, NodeEngine::TupleBufferPtr buffer) : schema(schema), buffer(buffer){};

SinkFormatIterator::iteratorPtr JsonFormatIterator::begin() {
    return std::make_shared<JsonFormatIterator::jsonIterator>(jsonIterator(buffer, schema, 0));
}

SinkFormatIterator::iteratorPtr JsonFormatIterator::end() {
    return std::make_shared<JsonFormatIterator::jsonIterator>(jsonIterator(buffer, schema, buffer->getNumberOfTuples()-1));
}

JsonFormatIterator::jsonIterator::jsonIterator(NodeEngine::TupleBufferPtr buffer, SchemaPtr schema, uint64_t index) {
    bufferPtr = buffer,
    schemaPtr = schema,
    bufferIndex = index;
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    // Iterate over all fields of a tuple. Store sizes in fieldOffsets, to calculate correct offsets in the step below.
    // Also, store types of fields in a separate array. Is later used to convert values to strings correctly.
    for (uint32_t i = 0; i < schemaPtr->getSize(); ++i) {
        auto physicalType = physicalDataTypeFactory.getPhysicalType(schemaPtr->get(i)->getDataType());
        fieldOffsets.push_back(physicalType->size());
        types.push_back(physicalType);
        NES_TRACE("CodeGenerator: " + std::string("Field Size ") + schemaPtr->get(i)->toString() + std::string(": ")
                      + std::to_string(physicalType->size()));
    }

    // Iteratively add up all the sizes in the offset array, to correctly determine where each field starts in the TupleBuffer
    // From fieldOffsets = {64, 32, 32, 128} (field sizes) to fieldOffsets = {64, 96, 128, 256} (actual field offsets)
    uint32_t prefix_sum = 0;
    for (uint32_t i = 0; i < fieldOffsets.size(); ++i) {
        uint32_t val = fieldOffsets[i];
        fieldOffsets[i] = prefix_sum;
        prefix_sum += val;
        NES_TRACE("CodeGenerator: " + std::string("Prefix SumAggregationDescriptor: ") + schemaPtr->get(i)->toString()
                      + std::string(": ") + std::to_string(fieldOffsets[i]));
    }
}
bool JsonFormatIterator::jsonIterator::operator!=(const JsonFormatIterator::jsonIteratorPtr other) const {
    if (bufferIndex == other->bufferIndex) {
        return false;
    };
    return true;
}

void JsonFormatIterator::jsonIterator::operator++() {
    if(bufferIndex == bufferPtr->getNumberOfTuples() -1) {
        NES_DEBUG("End of iterator reached. Can not proceed");
    } else {
        ++bufferIndex;
    }
}

std::string JsonFormatIterator::jsonIterator::getCurrentTupleAsJson() {
    uint8_t* tuplePointer = &this->bufferPtr->getBufferAs<uint8_t>()[bufferIndex * schemaPtr->getSchemaSizeInBytes()];
    std:: string jsonMessage = "{";

    // Add first tuple to json object and if there is more, add rest.
    // Adding the first tuple before the loop avoids checking if last tuple is processed in order to omit "," after json value
    uint32_t currentField = 0;
    std::string fieldValue = types[currentField]->convertRawToString(&tuplePointer[fieldOffsets[currentField]]);
    jsonMessage += schemaPtr->fields[currentField]->getName() + ":" + fieldValue;

    // Iterate over all fields in a tuple. Get field offsets from fieldOffsets array. Use fieldNames as keys and TupleBuffer
    // values as the corresponding values
    for (++currentField; currentField < schemaPtr->fields.size(); ++currentField) {
        jsonMessage += ",";
        void* fieldPointer = &tuplePointer[fieldOffsets[currentField]];
        fieldValue = types[currentField]->convertRawToString(fieldPointer);
        jsonMessage += schemaPtr->fields[currentField]->getName() + ":" + fieldValue;
    }
    jsonMessage += "}";
    return jsonMessage;
}

SinkFormatIteratorTypes JsonFormatIterator::getSinkFormat() { return JSON_FORMAT_ITERATOR; }
}// namespace NES
