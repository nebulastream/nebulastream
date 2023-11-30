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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_MEMORYPROVIDER_MEMORYPROVIDER_HPP_

#include <Nautilus/Interface/Record.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

class MemoryProvider;
using MemoryProviderPtr = std::unique_ptr<MemoryProvider>;

/**
 * @brief Abstract parent class for providing memory.
 */
class MemoryProvider {
  public:
    virtual ~MemoryProvider();

    /**
     * @brief Helper function for creating a memory provider from the buffer size and the schema
     * @param bufferSize
     * @param schema
     * @return MemoryProvider
     */
    static MemoryProviderPtr createMemoryProvider(const uint64_t bufferSize, const SchemaPtr schema);

    /**
     * @brief Return the memory layout pointer used by the MemoryProvider.
     * @return MemoryLayouts::MemoryLayoutPtr: Pointer to the memory layout.
     */
    virtual MemoryLayouts::MemoryLayoutPtr getMemoryLayoutPtr() = 0;

    /**
     * @brief Read fields from a record using projections.
     * @param projections: Defines which fields of the record are accessed. Empty projections means all fields.
     * @param bufferAddress: Address of the memory buffer that contains the record.
     * @param recordIndex: Index of the specific value that is accessed by 'read'.
     * @return Nautilus::Record: A Nautilus record constructed using the given projections.
     */
    virtual Nautilus::Record read(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                                  Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                                  Nautilus::Value<Nautilus::UInt64>& recordIndex) const = 0;

    /**
     * @brief Write record.
     * @param recordIndex: Index of the specific value that is accessed by 'write'.
     * @param bufferAddress: Address of the memory buffer that contains the record.
     * @param rec record to write.
     */
    virtual void write(Nautilus::Value<NES::Nautilus::UInt64>& recordIndex,
                       Nautilus::Value<Nautilus::MemRef>& bufferAddress,
                       NES::Nautilus::Record& rec) const = 0;

    /**
     * @brief load a scalar value of type 'type' from memory using a memory reference 'memRef'
     * @param type Type of the value that is loaded from memory.
     * @param bufferReference Memory reference to buffer that is accessed.
     * @param fieldReference Memory reference to the specific field that should be loaded.
     * @return Nautilus::Value<> Loaded value casted to correct Nautilus Value of type 'type'.
     */
    Nautilus::Value<> load(const PhysicalTypePtr& type,
                           Nautilus::Value<Nautilus::MemRef>& bufferReference,
                           Nautilus::Value<Nautilus::MemRef>& fieldReference) const;
    /**
     * @brief store a scalar value of type 'type' from memory using a memory reference 'memRef'
     * @param type Type of the value that is loaded from memory.
     * @param bufferReference Memory reference to buffer that is accessed.
     * @param fieldReference Memory reference to the specific field that should be loaded.
     * @param value Memory reference to the specific field that should be stored.
     * @return Nautilus::Value<> Loaded value casted to correct Nautilus Value of type 'type'.
     */
    Nautilus::Value<> store(const PhysicalTypePtr& type,
                            Nautilus::Value<Nautilus::MemRef>& bufferReference,
                            Nautilus::Value<Nautilus::MemRef>& fieldReference,
                            Nautilus::Value<>& value) const;

    /**
     * @brief Checks if given RecordFieldIdentifier projections contains the given field index.
     * @param projections Projections for record fields.
     * @param fieldIndex Field index that is possibly accessed.
     * @return true if no projections (entire record accessed) or if field is in projections, else return false.
     */
    [[nodiscard]] bool includesField(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                                     const Nautilus::Record::RecordFieldIdentifier& fieldIndex) const;
};

}// namespace NES::Runtime::Execution::MemoryProvider
#endif  // NES_RUNTIME_INCLUDE_EXECUTION_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
