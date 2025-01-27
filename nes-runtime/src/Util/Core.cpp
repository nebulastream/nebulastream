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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>


namespace NES
{

std::string Util::printTupleBufferAsText(Memory::TupleBuffer& buffer)
{
    std::stringstream ss;
    for (uint64_t i = 0; i < buffer.getNumberOfTuples(); i++)
    {
        ss << buffer.getBuffer<char>()[i];
    }
    return ss.str();
}

std::string Util::printTupleBufferAsCSV(Memory::TupleBuffer tbuffer, const SchemaPtr& schema)
{
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto* buffer = tbuffer.getBuffer<char>();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint64_t i = 0; i < numberOfTuples; i++)
    {
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getFieldCount(); j++)
        {
            auto field = schema->getFieldByIndex(j);
            auto dataType = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(dataType);
            auto fieldSize = physicalType->size();
            std::string str;
            auto indexInBuffer = buffer + offset + i * schema->getSchemaSizeInBytes();

            /// handle variable-length field
            if (NES::Util::instanceOf<VariableSizedDataType>(dataType))
            {
                NES_DEBUG("Util::printTupleBufferAsCSV(): trying to read the variable length VARIABLE_SIZED_DATA field: "
                          "from the tuple buffer");

                /// read the child buffer index from the tuple buffer
                auto childIdx = *reinterpret_cast<const uint32_t*>(indexInBuffer);
                str = Memory::MemoryLayouts::readVarSizedData(tbuffer, childIdx);
            }
            else
            {
                str = physicalType->convertRawToString(indexInBuffer);
            }

            ss << str;
            if (j < schema->getFieldCount() - 1)
            {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

std::string Util::toCSVString(const SchemaPtr& schema)
{
    return schema->toString("", ",", "\n");
}

std::shared_ptr<NES::Memory::MemoryLayouts::MemoryLayout> Util::createMemoryLayout(SchemaPtr schema, uint64_t bufferSize)
{
    switch (schema->getLayoutType())
    {
        case Schema::MemoryLayoutType::ROW_LAYOUT:
            return Memory::MemoryLayouts::RowLayout::create(schema, bufferSize);
        case Schema::MemoryLayoutType::COLUMNAR_LAYOUT:
            return Memory::MemoryLayouts::ColumnLayout::create(schema, bufferSize);
    }
}

bool Util::assignPropertiesToQueryOperators(const QueryPlanPtr& queryPlan, std::vector<std::map<std::string, std::any>> properties)
{
    /// count the number of operators in the query
    auto queryPlanIterator = PlanIterator(*queryPlan);
    size_t numOperators = queryPlanIterator.snapshot().size();
    ;

    /// check if we supply operator properties for all operators
    if (numOperators != properties.size())
    {
        NES_ERROR(
            "UtilityFunctions::assignPropertiesToQueryOperators: the number of properties does not match the number of "
            "operators. The query plan is: {}",
            queryPlan->toString());
        return false;
    }

    /// prepare the query plan iterator
    auto propertyIterator = properties.begin();

    /// iterate over all operators in the query
    for (auto&& node : queryPlanIterator)
    {
        for (const auto& [key, val] : *propertyIterator)
        {
            /// add the current property to the current operator
            NES::Util::as<LogicalOperator>(node)->addProperty(key, val);
        }
        ++propertyIterator;
    }

    return true;
}

std::vector<PhysicalTypePtr> Util::getPhysicalTypes(SchemaPtr schema)
{
    std::vector<PhysicalTypePtr> retVector;

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    for (const auto& field : *schema)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        retVector.push_back(physicalField);
    }
    return retVector;
}

#ifdef WRAP_READ_CALL
/// If NES is build with NES_ENABLES_TESTS the linker is instructed to wrap the read function
/// to keep the usual functionality __wrap_read just calls __real_read which is the real read function.
/// However, this allows to mock calls to read (e.g. TCPSourceTest)
extern "C" ssize_t __real_read(int fd, void* data, size_t size);
__attribute__((weak)) extern "C" ssize_t __wrap_read(int fd, void* data, size_t size)
{
    return __real_read(fd, data, size);
}
#endif
}
