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

#pragma once

#include <vector>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// The physical layout of the synopsis ref is the following
/// | --- Total Size Of Mem Area --- | --- Meta Data Size --- | --- Meta Data ---     | --- Synopsis ---    |
/// | ----------- 32bit -----------  | ------- 32bit -------  | --- metaDataSize ---  | --- totalSizeOfMemArea - metaDataSize ---  |

class SynopsisFunctionRef
{
public:
    /// @brief This constructor is used in the build phase to write records to the given memory and return the sample as a VariableSizedData object
    /// @param schema schema that matches the records that are supposed to be stored
    explicit SynopsisFunctionRef(const Schema& schema);

    /// @brief The synopsis ref must be initialized in the read phase to read records from the given memory
    /// @param memArea memory area that contains the synopsis
    void initializeForReading(const nautilus::val<int8_t*>& memArea);

    /// @brief This function initializes the synopsis ref for writing. It allocates memory for the synopsis and meta data
    /// @param arena reference to arena object
    /// @param synopsisDataSize accumulated data size of all records that are supposed to be stored
    /// (var sized data must account for actual data size plus sizeof uint32_t)
    /// @param synopsisMetaDataSize accumulated data size of the metadata that will be stored
    void initializeForWriting(
        ArenaRef& arena,
        const nautilus::val<uint32_t>& synopsisDataSize,
        const nautilus::val<uint32_t>& synopsisMetaDataSize,
        const VarVal& synopsisMetaData);

    /// Writes the record to the memory address currently pointed to by currWritePtr and
    /// increases it by the size of the record that was written
    void writeRecord(const Record& record);

    /// Reads a record from the memory address currently pointed to by currReadPtr and
    /// increases it by the size of the record that was read
    [[nodiscard]] Record readNextRecord();

    /// Sets the internal readPtr for readRecord to the tuple address corresponding to the given index
    void setReadPtrToRecordIdx(const nautilus::val<uint64_t>& idx);

    /// Returns the synopsis as a VarSizedData object
    [[nodiscard]] VariableSizedData getSynopsis() const;

    /// Returns the metadata as a VarSizedData object
    [[nodiscard]] VariableSizedData getMetaData() const;

private:
    Schema schema;
    nautilus::val<uint64_t> totalMemAreaSize;
    nautilus::val<int8_t*> startOfMemArea;
    nautilus::val<int8_t*> currWritePtr;
    nautilus::val<int8_t*> currReadPtr;
    bool isInitializedForWriting = false;
    bool isInitializedForReading = false;
};

}
