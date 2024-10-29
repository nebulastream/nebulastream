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
#include <sstream>
#include <string>
#include <Configurations/ConfigurationsNames.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>


namespace NES::Sinks
{

class SinkPrint : public Sink
{
public:
    static inline std::string NAME = "Print";

    explicit SinkPrint(const SinkDescriptor& sinkDescriptor);
    ~SinkPrint() override = default;

    void open() override { /* noop */ };
    void close() override { /* noop */ };

    bool emitTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer) override;
    static std::unique_ptr<Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string>&& config);

protected:
    std::ostream& toString(std::ostream& str) const override;
private:
    std::ostream& outputStream;
    std::unique_ptr<CSVFormat> outputParser;
};

/// Todo #355 : combine configuration with source configuration (get rid of duplicated code)
struct ConfigParametersPrint
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, Configurations::InputFormat>
        INPUT_FORMAT{"inputFormat", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
                         return Configurations::DescriptorConfig::tryGet(INPUT_FORMAT, config);
                     }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(INPUT_FORMAT);
};

}
