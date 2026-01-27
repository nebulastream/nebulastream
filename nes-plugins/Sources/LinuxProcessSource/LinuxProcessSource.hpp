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

#include <cstdio>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

struct ConfigParametersLinuxProcessSource
{
    static inline const DescriptorConfig::ConfigParameter<std::string> COMMAND{
        "command",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(COMMAND, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, COMMAND);
};

class LinuxProcessSource : public Source
{
public:
    static const std::string& name()
    {
        static const std::string Instance = "LinuxProcess";
        return Instance;
    }

    explicit LinuxProcessSource(const SourceDescriptor& sourceDescriptor);
    ~LinuxProcessSource() override = default;

    LinuxProcessSource(const LinuxProcessSource&) = delete;
    LinuxProcessSource& operator=(const LinuxProcessSource&) = delete;
    LinuxProcessSource(LinuxProcessSource&&) = delete;
    LinuxProcessSource& operator=(LinuxProcessSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string commandToRun;
    FILE* pipe = nullptr;
};

}