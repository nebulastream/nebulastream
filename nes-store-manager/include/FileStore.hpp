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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <BinaryStoreWriter.hpp>
#include <FlushPolicy.hpp>
#include <Store.hpp>
#include <StoreTransformation.hpp>

namespace NES::StoreManager
{

class ReplayStoreReader;

/// File-backed store wrapping BinaryStoreWriter/ReplayStoreReader. Satisfies StoreConcept.
/// Optionally chains to a next-level store with a flush policy and transformation.
class FileStore
{
public:
    struct Config
    {
        std::string storeName;
        std::string storeDir;
        std::string schemaText;
    };

    /// Standalone constructor (no chaining).
    explicit FileStore(Config config, const Schema& schema);

    /// Chained constructor: this store flushes to nextLevel when the policy triggers.
    FileStore(Config config, const Schema& schema, Store nextLevel, StoreTransformation transformation, FlushPolicy policy);

    ~FileStore();

    FileStore(const FileStore&) = delete;
    FileStore& operator=(const FileStore&) = delete;
    FileStore(FileStore&&) = delete;
    FileStore& operator=(FileStore&&) = delete;

    void open();
    void close(Store& self);
    void flush(Store& self);

    void write(TupleBuffer buffer, const Schema& schema, Store& self);
    uint64_t read(TupleBuffer& buffer, const Schema& schema);
    [[nodiscard]] bool hasMore() const;

    [[nodiscard]] Schema getSchema() const;
    [[nodiscard]] uint64_t size() const;

    [[nodiscard]] static std::string_view typeName() noexcept { return "FileStore"; }

    [[nodiscard]] const std::string& getFilePath() const { return filePath; }

    void removeFile();

private:
    /// Calculate packed row width from schema (no padding, matching binary format).
    static uint32_t calculateRowWidth(const Schema& schema);

    Config config;
    Schema schema;
    std::string filePath;
    BinaryStoreWriter writer;
    std::unique_ptr<ReplayStoreReader> reader;
    bool writerOpened{false};

    /// Chaining support (all optional — empty for standalone/tail stores).
    std::optional<Store> nextLevel;
    std::optional<StoreTransformation> transformation;
    std::optional<FlushPolicy> flushPolicy;
};

}
