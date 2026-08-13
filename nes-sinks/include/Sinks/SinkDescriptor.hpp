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
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <Configurations/ConfigField.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
class SinkCatalog;
}

namespace NES
{

/// The sink-defined config struct (e.g. FileSinkConfig), type-erased, together with the sink type
/// that identifies the SinkConfigRegistry entry that produced it (and can serialize it).
class PluginSinkConfiguration
{
public:
    PluginSinkConfiguration(Identifier type, ExplicitAny pluginData) : type(std::move(type)), pluginData(std::move(pluginData)) { }

    [[nodiscard]] const Identifier& getType() const { return type; }

    [[nodiscard]] const ExplicitAny& getPluginData() const { return pluginData; }

private:
    Identifier type;
    ExplicitAny pluginData;
};

class NamedSinkDescriptor final
{
    friend SinkCatalog;

public:
    ~NamedSinkDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const NamedSinkDescriptor& sinkDescriptor);
    friend bool operator==(const NamedSinkDescriptor& lhs, const NamedSinkDescriptor& rhs);

    [[nodiscard]] std::string getFormatType() const;
    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] Host getHost() const;
    [[nodiscard]] std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>> getSchema() const;
    [[nodiscard]] Identifier getSinkName() const;
    [[nodiscard]] bool getAddTimestamp() const;
    [[nodiscard]] size_t getBackpressureUpperThreshold() const;
    [[nodiscard]] size_t getBackpressureLowerThreshold() const;
    [[nodiscard]] const PluginSinkConfiguration& getPluginSinkConfiguration() const;
    [[nodiscard]] const OutputFormatterDescriptor& getOutputFormatterDescriptor() const;

private:
    explicit NamedSinkDescriptor(
        Identifier name,
        Schema<UnqualifiedUnboundField, Ordered> nameWithSchema,
        Host host,
        bool addTimestamp,
        size_t backpressureUpperThreshold,
        size_t backpressureLowerThreshold,
        PluginSinkConfiguration pluginSinkConfig,
        OutputFormatterDescriptor outputFormatterDescriptor);

    Identifier name;
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>> schema;
    Host host;
    bool addTimestamp;
    size_t backpressureUpperThreshold;
    size_t backpressureLowerThreshold;
    PluginSinkConfiguration pluginSinkConfig;
    OutputFormatterDescriptor outputFormatterDescriptor;

    friend Unreflector<NamedSinkDescriptor>;
};

using AnonymousSinkSchema = std::variant<
    std::monostate,
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>;

class AnonymousSinkDescriptor final
{
    friend SinkCatalog;
    friend struct SinkLogicalOperator;
    friend class CalcTargetOrderRule;

public:
    ~AnonymousSinkDescriptor() = default;


    friend std::ostream& operator<<(std::ostream& out, const AnonymousSinkDescriptor& sinkDescriptor);
    friend bool operator==(const AnonymousSinkDescriptor& lhs, const AnonymousSinkDescriptor& rhs);

    [[nodiscard]] std::string getFormatType() const;
    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] AnonymousSinkSchema getSchema() const;
    [[nodiscard]] uint64_t getSinkId() const;
    [[nodiscard]] Host getHost() const;
    [[nodiscard]] bool getAddTimestamp() const;
    [[nodiscard]] size_t getBackpressureUpperThreshold() const;
    [[nodiscard]] size_t getBackpressureLowerThreshold() const;
    [[nodiscard]] const PluginSinkConfiguration& getPluginSinkConfiguration() const;
    [[nodiscard]] const OutputFormatterDescriptor& getOutputFormatterDescriptor() const;

private:
    explicit AnonymousSinkDescriptor(
        uint64_t sinkId,
        AnonymousSinkSchema schema,
        Host host,
        bool addTimestamp,
        size_t backpressureUpperThreshold,
        size_t backpressureLowerThreshold,
        PluginSinkConfiguration pluginSinkConfig,
        OutputFormatterDescriptor outputFormatterDescriptor);

    [[nodiscard]] AnonymousSinkDescriptor withSchemaOrder(const Schema<UnqualifiedUnboundField, Ordered>& newSchema) const;

    uint64_t sinkId;
    AnonymousSinkSchema schema;
    Host host;
    bool addTimestamp;
    size_t backpressureUpperThreshold;
    size_t backpressureLowerThreshold;
    PluginSinkConfiguration pluginSinkConfig;
    OutputFormatterDescriptor outputFormatterDescriptor;

    friend Unreflector<AnonymousSinkDescriptor>;
};

class SinkDescriptor final
{
    friend SinkCatalog;
    friend Unreflector<SinkDescriptor>;
    friend class CalcTargetOrderRule;

public:
    ~SinkDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor);
    friend bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs);

    /// The output formatter type; "Native" if the sink forwards buffers without formatting.
    [[nodiscard]] std::string getFormatType() const;
    [[nodiscard]] std::string getSinkType() const;
    [[nodiscard]] AnonymousSinkSchema getSchema() const;
    [[nodiscard]] Identifier getSinkName() const;
    [[nodiscard]] bool isAnonymous() const;
    [[nodiscard]] Host getHost() const;
    [[nodiscard]] bool getAddTimestamp() const;
    [[nodiscard]] size_t getBackpressureUpperThreshold() const;
    [[nodiscard]] size_t getBackpressureLowerThreshold() const;
    /// The sink-defined config struct (e.g. FileSinkConfig), type-erased. Produced by the sink's
    /// SinkConfigRegistry entry, so the sink factory can safely any_cast it back.
    [[nodiscard]] const ExplicitAny& getPluginData() const;
    [[nodiscard]] const PluginSinkConfiguration& getPluginSinkConfiguration() const;
    [[nodiscard]] const OutputFormatterDescriptor& getOutputFormatterDescriptor() const;
    [[nodiscard]] const std::variant<NamedSinkDescriptor, AnonymousSinkDescriptor>& getUnderlying() const;

private:
    explicit SinkDescriptor(std::variant<NamedSinkDescriptor, AnonymousSinkDescriptor> underlying);
    std::variant<NamedSinkDescriptor, AnonymousSinkDescriptor> underlying;

    friend Reflector<SinkDescriptor>;

public:
    /// General sink config fields, shared by all sink types. Host determines worker placement;
    /// the (optional) schema types the sink's input for anonymous sinks declared inside a query.
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<Host> HOST{
        Identifier::parse("HOST"),
        "Where to place the sink, in the form of host:port . Must be a NES node that is part of the topology.",
        [](const ConfigLiteral& literal) -> std::expected<Host, Exception>
        {
            return tryGetOr<std::string>(literal, expectedType<std::string>())
                .transform([](std::string&& value) -> Host { return Host{std::move(value)}; });
        },
        Host{Host::INVALID}};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<std::optional<Schema<UnqualifiedUnboundField, Ordered>>> SCHEMA{
        Identifier::parse("SCHEMA"),
        "The schema of the sink, only specifiable as an option in anonymous sources.",
        [](const ConfigLiteral& literal)
        { return tryGetOr<Schema<UnqualifiedUnboundField, Ordered>>(literal, expectedType<Schema<UnqualifiedUnboundField, Ordered>>()); },
        std::nullopt};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<bool> ADD_TIMESTAMP{
        Identifier::parse("ADD_TIMESTAMP"), "A boolean indicating whether to add a timestamp to tuples", false};

    /// TODO #1964: move this to network sink
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<size_t> BACKPRESSURE_UPPER_THRESHOLD{
        Identifier::parse("BACKPRESSURE_UPPER_THRESHOLD"),
        "The number of buffers a sink is not able to emit, that needs to be reached before it starts applying backpressure.",
        [](const ConfigLiteral& literal)
        { return tryGetOr<int64_t>(literal, expectedType<size_t>()).and_then(narrowConfigValue<int64_t, size_t>); },
        size_t{1000}};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const ConfigField<size_t> BACKPRESSURE_LOWER_THRESHOLD{
        Identifier::parse("BACKPRESSURE_LOWER_THRESHOLD"),
        "The number of buffers a sink a sink is not able to emit, that needs to be reached so that the sink stops applying backpressure.",
        [](const ConfigLiteral& literal)
        { return tryGetOr<int64_t>(literal, expectedType<size_t>()).and_then(narrowConfigValue<int64_t, size_t>); },
        size_t{200}};

    static inline auto configSchema = createConfigSchema(
        Identifier::parse("SINK"), HOST, SCHEMA, ADD_TIMESTAMP, BACKPRESSURE_UPPER_THRESHOLD, BACKPRESSURE_LOWER_THRESHOLD);

    friend struct SinkLogicalOperator;
};

template <>
struct Reflector<SinkDescriptor>
{
    Reflected operator()(const SinkDescriptor& descriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<SinkDescriptor>
{
    SinkDescriptor operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<PluginSinkConfiguration>
{
    Reflected operator()(const PluginSinkConfiguration& config, const ReflectionContext& context) const;
};

template <>
struct Unreflector<PluginSinkConfiguration>
{
    PluginSinkConfiguration operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

namespace detail
{
struct ReflectedAnonymousSinkDescriptor
{
    uint64_t sinkId;
    std::variant<std::monostate, Schema<UnqualifiedUnboundField, Unordered>, Schema<UnqualifiedUnboundField, Ordered>> schema;
    Host host;
    bool addTimestamp;
    size_t backpressureUpperThreshold;
    size_t backpressureLowerThreshold;
    PluginSinkConfiguration pluginSinkConfig;
    OutputFormatterDescriptor outputFormatterDescriptor;
};

struct ReflectedNamedSinkDescriptor
{
    Identifier name;
    Schema<UnqualifiedUnboundField, Ordered> schema;
    Host host;
    bool addTimestamp;
    size_t backpressureUpperThreshold;
    size_t backpressureLowerThreshold;
    PluginSinkConfiguration pluginSinkConfig;
    OutputFormatterDescriptor outputFormatterDescriptor;
};
}

template <>
struct Reflector<NamedSinkDescriptor>
{
    Reflected operator()(const NamedSinkDescriptor& descriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<NamedSinkDescriptor>
{
    NamedSinkDescriptor operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<AnonymousSinkDescriptor>
{
    Reflected operator()(const AnonymousSinkDescriptor& descriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<AnonymousSinkDescriptor>
{
    AnonymousSinkDescriptor operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::SinkDescriptor>
{
    size_t operator()(const NES::SinkDescriptor& sinkDescriptor) const noexcept
    {
        return std::hash<NES::Identifier>{}(sinkDescriptor.getSinkName());
    }
};

FMT_OSTREAM(NES::SinkDescriptor);
