#ifndef NES_CLIOPTIONS_H
#define NES_CLIOPTIONS_H
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <YAMLModel.h>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast/try_lexical_convert.hpp>
#include <boost/outcome.hpp>
#include <optional>
#include <string>
#include <variant>
#include <yaml-cpp/yaml.h>

namespace NES {
class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source
}// namespace NES

struct ExportKafkaConfiguration {
    std::string broker;
    std::string topic;
};

struct ExportTopologyNodeConfiguration {
    std::string ip;
    size_t port;
    size_t resources;
};

struct TCPSourceConfiguration {
    std::string ip;
    size_t port;

    friend bool operator==(const TCPSourceConfiguration& lhs, const TCPSourceConfiguration& rhs) {
        return lhs.ip == rhs.ip && lhs.port == rhs.port;
    }

    friend bool operator!=(const TCPSourceConfiguration& lhs, const TCPSourceConfiguration& rhs) { return !(lhs == rhs); }
};

struct ExportSourceConfiguration {
    std::string name;
    SchemaConfiguration schema;
    std::optional<TCPSourceConfiguration> tcp;
};

struct ExportWorkerConfiguration {
    ExportTopologyNodeConfiguration node;
    std::vector<std::variant<std::string, size_t>> links;
    std::vector<ExportSourceConfiguration> sources;
};

struct ExportTopologySinkConfiguration {
    std::optional<ExportKafkaConfiguration> kafka;
    std::optional<ExportTopologyNodeConfiguration> node;
};

struct ExportTopologyConfiguration {
    ExportTopologySinkConfiguration sink;
    std::vector<ExportWorkerConfiguration> workers;
};

struct ExportConfiguration {
    std::string query;
    ExportTopologyConfiguration topology;
};

#define UNIKERNEL_MODEL_YAML_DECODE_VARIANT(field, type_name, type_tag)                                                          \
    do {                                                                                                                         \
        if (node[#field]["type"].as<std::string>() == type_tag) {                                                                \
            rhs.field = node[#field].as<type_name>();                                                                            \
        }                                                                                                                        \
    } while (0)

#define UNIKERNEL_MODEL_YAML_ENCODE_VARIANT(field, type_name, type_tag)                                                          \
    do {                                                                                                                         \
        if (std::holds_alternative<type_name>(rhs.field)) {                                                                      \
            node[#field] = std::get<type_name>(rhs.field);                                                                       \
            node[#field]["type"] = type_tag;                                                                                     \
        }                                                                                                                        \
    } while (0)

namespace YAML {
template<>
struct convert<TCPSourceConfiguration> {
    static Node encode(const TCPSourceConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(ip);
        UNIKERNEL_MODEL_YAML_ENCODE(port);
        return node;
    }

    static Node decode(const Node& node, TCPSourceConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(ip);
        UNIKERNEL_MODEL_YAML_DECODE(port);
        return node;
    }
};

template<>
struct convert<ExportTopologyNodeConfiguration> {
    static Node encode(const ExportTopologyNodeConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(ip);
        UNIKERNEL_MODEL_YAML_ENCODE(port);
        UNIKERNEL_MODEL_YAML_ENCODE(resources);
        return node;
    }

    static Node decode(const Node& node, ExportTopologyNodeConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(ip);
        UNIKERNEL_MODEL_YAML_DECODE(port);
        UNIKERNEL_MODEL_YAML_DECODE(resources);
        return node;
    }
};

template<>
struct convert<ExportSourceConfiguration> {
    static Node encode(const ExportSourceConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(name);
        UNIKERNEL_MODEL_YAML_ENCODE(schema);
        if (rhs.tcp.has_value()) {
            node["tcp"] = *rhs.tcp;
        }
        return node;
    };

    static Node decode(const Node& node, ExportSourceConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(name);
        UNIKERNEL_MODEL_YAML_DECODE(schema);
        if (node["tcp"]) {
            rhs.tcp.emplace(node["tcp"].as<TCPSourceConfiguration>());
        }
        return node;
    }
};

template<>
struct convert<std::variant<std::string, size_t>> {
    static Node encode(const std::variant<std::string, size_t>& rhs) {
        Node node;
        if (holds_alternative<std::string>(rhs)) {
            node = get<std::string>(rhs);
        }
        if (holds_alternative<size_t>(rhs)) {
            node = get<size_t>(rhs);
        }
        return node;
    }

    static Node decode(const Node& node, std::variant<std::string, size_t>& rhs) {
        auto asString = node.as<std::string>();
        size_t asNumber = 0;
        if (boost::conversion::try_lexical_convert(asString, asNumber)) {
            rhs = asNumber;
        } else {
            rhs = asString;
        }
        return node;
    }
};

template<>
struct convert<ExportWorkerConfiguration> {
    static Node encode(const ExportWorkerConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(node);
        UNIKERNEL_MODEL_YAML_ENCODE(sources);
        UNIKERNEL_MODEL_YAML_ENCODE(links);
        return node;
    };

    static Node decode(const Node& node, ExportWorkerConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(node);

        if (node["sources"]) {
            UNIKERNEL_MODEL_YAML_DECODE(sources);
        }

        UNIKERNEL_MODEL_YAML_DECODE(links);
        return node;
    }
};

template<>
struct convert<ExportTopologySinkConfiguration> {
    static Node encode(const ExportTopologySinkConfiguration& rhs) {
        Node node;
        if (rhs.node.has_value()) {
            node["node"] = rhs.node.value();
        }
        if (rhs.kafka.has_value()) {
            node["kafka"] = rhs.kafka.value();
        }

        NES_ASSERT(rhs.node.has_value() && rhs.kafka.has_value(), "Neither Node nor Kafka are defined");
        return node;
    };

    static Node decode(const Node& node, ExportTopologySinkConfiguration& rhs) {
        if (node["node"]) {
            rhs.node = node["node"].as<ExportTopologyNodeConfiguration>();
        }
        if (node["kafka"]) {
            rhs.kafka = node["kafka"].as<ExportKafkaConfiguration>();
        }
        return node;
    }
};

template<>
struct convert<ExportTopologyConfiguration> {
    static Node encode(const ExportTopologyConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(sink);
        UNIKERNEL_MODEL_YAML_ENCODE(workers);
        return node;
    };

    static Node decode(const Node& node, ExportTopologyConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(sink);
        UNIKERNEL_MODEL_YAML_DECODE(workers);
        return node;
    }
};

template<>
struct convert<ExportConfiguration> {
    static Node encode(const ExportConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(query);
        UNIKERNEL_MODEL_YAML_ENCODE(topology);
        return node;
    };

    static Node decode(const Node& node, ExportConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(query);
        UNIKERNEL_MODEL_YAML_DECODE(topology);
        return node;
    }
};

template<>
struct convert<ExportKafkaConfiguration> {
    static Node encode(const ExportKafkaConfiguration& rhs) {
        Node node;
        UNIKERNEL_MODEL_YAML_ENCODE(broker);
        UNIKERNEL_MODEL_YAML_ENCODE(topic);
        return node;
    };

    static Node decode(const Node& node, ExportKafkaConfiguration& rhs) {
        UNIKERNEL_MODEL_YAML_DECODE(broker);
        UNIKERNEL_MODEL_YAML_DECODE(topic);
        return node;
    }
};
}// namespace YAML

class Options;
using CLIResult = boost::outcome_v2::result<Options, std::string>;

class Options {
  public:
    static CLIResult getCLIOptions(int argc, char** argv);
    std::string getQueryString();
    [[nodiscard]] bool useKafka() const;
    [[nodiscard]] size_t getBufferSize() const;
    std::string getOutputPathForFile(const std::string& file);
    void createSource(std::shared_ptr<NES::Catalogs::Source::SourceCatalog> sourceCatalog,
                      std::vector<NES::PhysicalSourceTypePtr> physicalSources,
                      NES::TopologyNodePtr worker,
                      const ExportSourceConfiguration& source);
    std::tuple<NES::TopologyPtr, NES::Catalogs::Source::SourceCatalogPtr, std::vector<NES::PhysicalSourceTypePtr>>
    getTopologyAndSources();
    [[nodiscard]] ExportKafkaConfiguration getKafkaConfiguration() const;
    [[nodiscard]] std::string getYAMLOutputPath() const;
    [[nodiscard]] boost::filesystem::path getStageOutputPathForNode(NES::NodeId) const;

  private:
    ExportConfiguration configuration;
    std::optional<std::string> input;
    size_t bufferSize = 8192;
    std::optional<std::string> output;
    std::string yamlOutput;
    std::optional<size_t> sourceNodeId;
    static size_t getOtherNodeIdFromLink(const std::variant<std::string, size_t>& variant);
};
#endif//NES_CLIOPTIONS_H
