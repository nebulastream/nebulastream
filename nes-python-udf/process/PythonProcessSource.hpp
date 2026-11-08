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

#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>

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

/// Starts the pipeline behind an out-of-process UDF operator, and owns its processes: the python of NES_UDF_PROCESS_VENV
/// (worker.python_udf.process_venv) or else of the worker's venv if one is configured, otherwise python3 on the PATH. Each process has a
/// GIL of its own, so the source starts several of them, as many as NES_UDF_PROCESSES says (worker.python_udf.processes), so that no
/// worker thread ever waits for Python. How batches reach the processes depends on the protocol NES_UDF_PROCESS_PROTOCOL names
/// (worker.python_udf.process_protocol), see UdfProcessProtocol: over their sockets for ARROW_IPC and RAW_BUFFERS, see
/// PythonProcessSocketSource, and through shared memory for SHARED_MEMORY and SHARED_MEMORY_ZERO_COPY, see PythonProcessShmSource.
class PythonProcessSource final : public Source
{
public:
    static constexpr std::string_view NAME = "PythonProcess";

    PythonProcessSource(const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor);

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    [[nodiscard]] bool providesBuffers() const override;
    std::optional<TupleBuffer> nextBuffer(const std::stop_token& stopToken) override;
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    /// Origin id, sequence and chunk number and watermark are taken over from the input.
    [[nodiscard]] bool addsMetadata() const override { return true; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::unique_ptr<Source> implementation;
};

}
