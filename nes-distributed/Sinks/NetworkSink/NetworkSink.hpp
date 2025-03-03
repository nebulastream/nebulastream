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

#include "NetworkSink.hpp"

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <network/lib.h>
#include <folly/Synchronized.h>
#include <rust/cxx.h>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

class NetworkSink final : public Sink
{
public:

    explicit NetworkSink(Valve valve, const SinkDescriptor& sinkDescriptor);
    ~NetworkSink() override = default;

    NetworkSink(const NetworkSink&) = delete;
    NetworkSink& operator=(const NetworkSink&) = delete;
    NetworkSink(NetworkSink&&) = delete;
    NetworkSink& operator=(NetworkSink&&) = delete;
    void start(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    void
    execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;

    static std::unique_ptr<Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string>&& config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    size_t tupleSize;
    folly::Synchronized<std::vector<Memory::TupleBuffer>> buffers;
    std::optional<rust::Box<SenderServer>> server{};
    std::optional<rust::Box<SenderChannel>> channel{};
    std::string channelIdentifier{};
    std::string connectionIdentifier;
};


}
