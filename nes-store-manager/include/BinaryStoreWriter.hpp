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
#include <cstddef>
#include <cstdint>
#include <string>

namespace NES::StoreManager
{
/// POSIX-based binary file writer for the Replay store format.
class BinaryStoreWriter
{
public:
    struct Config
    {
        std::string filePath;
        bool append{true};
        bool writeHeader{true};
        bool directIO{false};
        uint32_t fdatasyncInterval{0};
        std::string schemaText;
    };

    explicit BinaryStoreWriter(Config cfg);
    ~BinaryStoreWriter();

    BinaryStoreWriter(const BinaryStoreWriter&) = delete;
    BinaryStoreWriter& operator=(const BinaryStoreWriter&) = delete;
    BinaryStoreWriter(BinaryStoreWriter&&) = delete;
    BinaryStoreWriter& operator=(BinaryStoreWriter&&) = delete;

    /// Open the file for writing. Initializes the tail offset from current file size.
    void open();

    /// Fsync and close the file descriptor.
    void close();

    /// Thread-safe: write the file header exactly once (if file is empty and writeHeader is enabled).
    void ensureHeader();

    /// Append a contiguous buffer using atomic offset reservation and pwrite.
    void append(const uint8_t* data, size_t len);

    [[nodiscard]] const std::string& getFilePath() const { return config.filePath; }

private:
    int fd{-1};
    std::atomic<uint64_t> tail{0};
    std::atomic<bool> headerWritten{false};
    Config config;
    uint64_t writesSinceSync{0};
};

} // namespace NES::Replay
