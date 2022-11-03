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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMORYPROVIDER_MEMORYPROVIDER_HPP_

#include <Nautilus/Interface/Record.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

/**
 * @brief Abstract parent class for providing memory.
 */
class MemoryProvider {
  public:
    virtual ~MemoryProvider();

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
                          Nautilus::Value<Nautilus::MemRef> bufferAddress,
                          Nautilus::Value<Nautilus::UInt64> recordIndex) = 0;

    /**
     * @brief Read fields from a record using projections.
     * @param projections: Defines which fields of the record are accessed. Empty projections means all fields.
     * @param bufferAddress: Address of the memory buffer that contains the record.
     * @param recordIndex: Index of the specific value that is accessed by 'read'.
     * @return Nautilus::Record: A Nautilus record constructed using the given projections.
     */
    virtual void write(Nautilus::Value<NES::Nautilus::UInt64> recordIndex, 
                       Nautilus::Value<Nautilus::MemRef> bufferAddress, NES::Nautilus::Record& rec) = 0;

    /**
     * @brief load a scalar value of type 'type' from memory using a memory reference 'memRef'
     * @param type Type of the value that is loaded from memory.
     * @param memRef Memory reference to beginning of value that is loaded.
     * @return Nautilus::Value<> Loaded value casted to correct Nautilus Value of type 'type'.
     */
    Nautilus::Value<> load(PhysicalTypePtr type, Nautilus::Value<Nautilus::MemRef> memRef);

    /**
     * @brief Checks if given RecordFieldIdentifier projections contains the given field index.
     * @param projections Projections for record fields.
     * @param fieldIndex Field index that is possibly accessed.
     * @return true if no projections (entire record accessed) or if field is in projections, else return false.
     */
    bool includesField(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                            Nautilus::Record::RecordFieldIdentifier fieldIndex);
};

}// namespace NES::Runtime::Execution::MemoryProvider
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_MEMORYPROVIDER_MEMORYPROVIDER_HPP_
