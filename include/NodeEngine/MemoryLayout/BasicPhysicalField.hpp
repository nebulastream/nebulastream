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

#ifndef NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_BASICPHYSICALFIELD_HPP_
#define NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_BASICPHYSICALFIELD_HPP_
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/TupleBuffer.hpp>
namespace NES {
/**
 * @brief Represents an value field at a specific position in a memory buffer.
 * This class has to be implemented in the header as it is a template type.
 */
template<class ValueType>
class BasicPhysicalField : public PhysicalField {
  public:
    BasicPhysicalField(uint64_t bufferOffset) : PhysicalField(bufferOffset){};
    ~BasicPhysicalField(){};

    /**
     * @brief writes a value of type ValueType to a particular position in the buffer.
     * @param tupleBuffer target tuple buffer
     * @param value the value we want to write
     */
    void write(TupleBuffer& tupleBuffer, ValueType value) {
        auto byteBuffer = tupleBuffer.getBufferAs<int8_t>();
        // interpret the target address as value type and write value to tuple buffer(ValueType *) byteBuffer[multiplier]) = value;
        ((ValueType*) (&byteBuffer[bufferOffset]))[0] = value;
    };

    /**
     * Reads a value of type value type from the tuple buffer.
     * @param tupleBuffer
     * @return ValueType
     */
    ValueType read(TupleBuffer& tupleBuffer) {
        auto byteBuffer = tupleBuffer.getBufferAs<int8_t>();
        // interpret the target address as value type and read value from tuple buffer
        return ((ValueType*) (&byteBuffer[bufferOffset]))[0];
    }
};

template<class ValueType>
PhysicalFieldPtr createBasicPhysicalField(uint64_t bufferOffset) {
    return std::make_shared<BasicPhysicalField<ValueType>>(bufferOffset);
}
}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_BASICPHYSICALFIELD_HPP_
