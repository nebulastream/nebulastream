#ifndef NES_CLIOPTIONS_H
#define NES_CLIOPTIONS_H
#include <boost/outcome.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <optional>
#include <optional>
#include <string>
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

class Options;
using CLIResult = boost::outcome_v2::result<Options, std::string>;
class Options {
  public:
    static CLIResult getCLIOptions(int argc, char** argv);
    std::string getQueryString();
    size_t getBufferSize() const;
    std::string getOutputPathForFile(const std::string& file);
    std::tuple<NES::TopologyPtr, NES::Catalogs::Source::SourceCatalogPtr, std::vector<NES::PhysicalSourceTypePtr>>
    getTopologyAndSources();
    std::string getYAMLOutputPath() const;

  private:
    std::optional<YAML::Node> yamlRoot;
    std::optional<std::string> input;
    size_t bufferSize = 8192;
    std::optional<std::string> output;
    std::string yamlOutput;
    YAML::Node& loadInputYaml();
    std::optional<size_t> sourceNodeId;
    NES::SchemaPtr parseSchema(YAML::Node schemaNode);
};
#endif//NES_CLIOPTIONS_H
