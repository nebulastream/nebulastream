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

#include <atomic>
#include <cstdint>
#include <fstream>
#include <iosfwd>
#include <string>

#include <DataTypes/Schema.hpp>
#include <ReplayStoreFormat.hpp>

namespace NES::StoreManager
{

/// Reads binary store files produced by the Replay store writer.
class ReplayStoreReader
{
public:
    explicit ReplayStoreReader(std::string filePath);
    ~ReplayStoreReader();

    ReplayStoreReader(const ReplayStoreReader&) = delete;
    ReplayStoreReader& operator=(const ReplayStoreReader&) = delete;
    ReplayStoreReader(ReplayStoreReader&&) = delete;
    ReplayStoreReader& operator=(ReplayStoreReader&&) = delete;

    /// Open the file and parse the header.
    void open();

    /// Close the input stream.
    void close();

    /// Return the schema embedded in the file header.
    [[nodiscard]] Schema readSchema() const;

    /// Verify that the given expected schema matches the schema stored in the file header.
    /// Throws on mismatch.
    void verifySchema(const Schema& expectedSchema) const;

    /// Read fixed-size rows from the file into a destination buffer.
    /// Handles both fixed-width and variable-sized fields per the binary format.
    /// Returns the number of complete rows successfully read.
    uint64_t readRows(char* dest, uint64_t maxRows, uint32_t tupleSize, const Schema& schema);

    /// Check whether the end of file has been reached.
    [[nodiscard]] bool isEof() const;

    /// Get the byte offset where data begins (after the header).
    [[nodiscard]] uint64_t getDataStartOffset() const { return dataStartOffset; }

    /// Get the current stream position.
    [[nodiscard]] std::streampos getPosition();

    /// Get total bytes read so far.
    [[nodiscard]] uint64_t getTotalBytesRead() const { return totalBytesRead.load(); }

    /// Clear stream error flags (e.g., after hitting EOF).
    void clearErrors();

    /// Seek to a specific position in the file.
    void seekTo(std::streampos pos);

    /// Peek the next character without consuming it. Returns EOF trait if at end.
    [[nodiscard]] int peek();

    /// Static helper: read schema from a file without keeping it open.
    static Schema readSchemaFromFile(const std::string& filePath);

private:
    std::string filePath;
    std::ifstream inputFile;
    FileHeader header;
    uint64_t dataStartOffset{0};
    std::atomic<uint64_t> totalBytesRead{0};
    bool opened{false};
};

}
