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

#ifndef INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALSCHEMA_HPP_
#define INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALSCHEMA_HPP_
#include <Common/DataTypes/DataType.hpp>
#include <memory>
#include <vector>

namespace NES{
class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;
}
namespace NES::NodeEngine {

template<class ValueType>
class BasicPhysicalField;

class PhysicalField;
typedef std::shared_ptr<PhysicalField> PhysicalFieldPtr;

class PhysicalSchema;
typedef std::shared_ptr<PhysicalSchema> PhysicalSchemaPtr;

/**
 * @brief the physical schema which maps a logical schema do physical fields, which can be accessed dynamically.

 */
class PhysicalSchema {
  public:
    explicit PhysicalSchema(SchemaPtr schemaPtr);
    static PhysicalSchemaPtr createPhysicalSchema(SchemaPtr schema);

    /**
     * @brief returns size of one record in bytes.
     * @return uint64_t size in bytes
     */
    uint64_t getRecordSize();

    /**
     * @brief Calculated the offset of a particular field in a record.
     * @param fieldIndex index of the field we want to access.
     * @throws IllegalArgumentException if fieldIndex is not valid
     * @return offset in byte
     */
    uint64_t getFieldOffset(uint64_t fieldIndex);

    /**
     * Get the physical field representation at a field index
     * @throws IllegalArgumentException if fieldIndex is not valid
     * @param fieldIndex
     */
    PhysicalFieldPtr createPhysicalField(uint64_t fieldIndex, uint64_t bufferOffset);

  private:
    SchemaPtr schema;
    /**
     * @brief Checks if the fieldIndex is in contained in the schema
     * @param fieldIndex
     * @return true if fieldIndex is valid
     */
    bool validFieldIndex(uint64_t fieldIndex);
};

}// namespace NES

#endif//INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALSCHEMA_HPP_
