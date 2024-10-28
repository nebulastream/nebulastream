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

#include <string>

#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sinks
{

class Sink
{
public:
    Sink(const QueryId queryId) : queryId(queryId) {};
    virtual ~Sink() = default;

    /// Todo: make input of interface function abstract. As a result, we can easily switch out the type of input, without needing to change every single source.
    /// - could do: SourceInput that has a generic interface.
    virtual bool emitTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer) = 0;

    /// Not purely virtual, because some sinks, e.g., SinkPrint, don't need to open/close.
    virtual void open() = 0;
    virtual void close() = 0;

    friend std::ostream& operator<<(std::ostream& out, const Sink& sink);
    friend bool operator==(const Sink& lhs, const Sink& rhs);

    const QueryId queryId;

protected:
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
    [[nodiscard]] virtual bool equals(const Sink& other) const = 0;

    /// Todo: if validation works, sources and sinks should share validation, to avoid copy&paste
    /// Make sure that SinkSpecificConfiguration::parameterMap exists.
    template <typename T, typename = void>
    struct has_parameter_map : std::false_type
    {
    };
    template <typename T>
    struct has_parameter_map<T, std::void_t<decltype(std::declval<T>().parameterMap)>> : std::true_type
    {
    };
    /// Iterates over all parameters in a user provided config and checks if they are supported by a sink-specific config.
    /// Then iterates over all supported config parameters, validates and formats the strings provided by the user.
    /// Uses default parameters if the user did not specify a parameter.
    /// @throws If a mandatory parameter was not provided, an optional parameter was invalid, or a not-supported parameter was encountered.
    template <typename SinkSpecificConfiguration>
    static Configurations::DescriptorConfig::Config
    validateAndFormatImpl(std::unordered_map<std::string, std::string>&& config, const std::string_view sinkName)
    {
        Configurations::DescriptorConfig::Config validatedConfig;

        /// First check if all user-specified keys are valid.
        for (const auto& [key, _] : config)
        {
            static_assert(
                has_parameter_map<SinkSpecificConfiguration>::value,
                "A sink configuration must have a parameterMap containing all configuration parameters.");
            if (key != "type" && not SinkSpecificConfiguration::parameterMap.contains(key))
            {
                throw(InvalidConfigParameter(fmt::format("Unknown configuration parameter: {}.", key)));
            }
        }
        /// Next, try to validate all config parameters.
        for (const auto& [key, configParameter] : SinkSpecificConfiguration::parameterMap)
        {
            for (const auto& [keyString, configParameterString] : config)
            {
                NES_DEBUG("key: {}, value: {}", keyString, configParameterString);
            }
            const auto validatedParameter = configParameter.validate(config);
            if (validatedParameter.has_value())
            {
                validatedConfig.emplace(key, validatedParameter.value());
                continue;
            }
            /// If the user did not specify a parameter that is optional, use the default value.
            if (not config.contains(key) && configParameter.getDefaultValue().has_value())
            {
                validatedConfig.emplace(key, configParameter.getDefaultValue().value());
                continue;
            }
            throw InvalidConfigParameter(fmt::format("Failed validation of config parameter: {}, in Sink: {}", key, sinkName));
        }
        return validatedConfig;
    }
};

}

namespace fmt
{
template <>
struct formatter<NES::Sinks::Sink> : ostream_formatter
{
};
}
