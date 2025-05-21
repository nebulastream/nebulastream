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

#include "NetworkSource.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <network/lib.h>
#include <Bridge.hpp>

namespace NES::Sources
{

class NetworkSource : public Source
{
public:
    explicit NetworkSource(const SourceDescriptor& sourceDescriptor);
    ~NetworkSource() override = default;

    NetworkSource(const NetworkSource&) = delete;
    NetworkSource& operator=(const NetworkSource&) = delete;
    NetworkSource(NetworkSource&&) = delete;
    NetworkSource& operator=(NetworkSource&&) = delete;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<Memory::AbstractBufferProvider> provider) override;
    void close() override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    bool fillBuffer(NES::Memory::TupleBuffer& tupleBuffer, size_t& numReceivedBytes);
    std::string channelIdentifier;
    std::optional<rust::Box<ReceiverChannel>> channel{};
    rust::Box<ReceiverServer> receiverServer;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider{};
};

}
