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
#include <expected>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct UnixSocketSourceConfig
{
    std::string path;

    static std::expected<UnixSocketSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Reads an opaque byte stream from an existing Unix-domain stream socket.
/// The peer owns the listening socket and its filesystem path.
class UnixSocketSource final : public Source
{
public:
    static constexpr std::string_view NAME = "unix_socket";

    explicit UnixSocketSource(const UnixSocketSourceConfig& config);
    ~UnixSocketSource() override;

    UnixSocketSource(const UnixSocketSource&) = delete;
    UnixSocketSource& operator=(const UnixSocketSource&) = delete;
    UnixSocketSource(UnixSocketSource&&) = delete;
    UnixSocketSource& operator=(UnixSocketSource&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider>) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    std::string path;
    int descriptor{-1};
    std::atomic<size_t> totalNumBytesRead{0};
};

}
