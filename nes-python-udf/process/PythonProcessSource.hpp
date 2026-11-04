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

#include <cstddef>
#include <expected>
#include <memory>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include <sys/types.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <PythonProcessChannel.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

struct PythonProcessSourceConfig
{
    /// Id of the channel the `PythonProcess` sink of the same UDF operator hands its buffers to.
    std::string channel;
    /// The serialized calls of the UDF operator, see UdfProcessCall.
    std::string calls;

    static std::expected<PythonProcessSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Starts the pipeline behind an out-of-process UDF operator, and owns its process: the worker's venv's python if one is configured,
/// otherwise python3 on the PATH. The source thread sends one batch at a time and waits for it, so no worker thread ever waits for
/// Python. The process answers with a batch of the same layout, which the source passes on with the metadata of the input.
class PythonProcessSource final : public Source
{
public:
    static constexpr std::string_view NAME = "PythonProcess";

    PythonProcessSource(const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor);
    ~PythonProcessSource() override;
    PythonProcessSource(const PythonProcessSource&) = delete;
    PythonProcessSource& operator=(const PythonProcessSource&) = delete;
    PythonProcessSource(PythonProcessSource&&) = delete;
    PythonProcessSource& operator=(PythonProcessSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Origin id, sequence and chunk number and watermark are taken over from the input.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    void startProcess();
    /// Return false on a stop request.
    bool sendAll(std::span<const std::byte> bytes, const std::stop_token& stopToken) const;
    bool receiveAll(std::span<std::byte> bytes, const std::stop_token& stopToken) const;
    bool receiveStatus(const std::stop_token& stopToken) const;
    bool sendBatch(const TupleBuffer& input, const std::stop_token& stopToken) const;
    bool receiveBatch(TupleBuffer& output, const std::stop_token& stopToken) const;

    PythonProcessChannel::Lease channel;
    std::vector<UdfProcessCall> calls;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::shared_ptr<ArrowBufferRef> layout;
    std::vector<ArrowBufferRef::Field> varSizedFields;
    int processSocket = -1;
    pid_t process = -1;
};

}
