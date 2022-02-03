#include <Configurations/Coordinator/NewCoordinatorConfiguration.hpp>

namespace NES::Configurations {

LogicalSourcePtr LogicalSourceFactory::createFromString(std::string) {
    throw ConfigurationException("LogicalSources cant be configured by a command");
}

LogicalSourcePtr LogicalSourceFactory::createFromYaml(Yaml::Node node) {
    if (!node.IsMap()) {
        throw ConfigurationException("LogicalSources must be defined as a map");
    }

    auto logicalSourceName = node["logicalSourceName"].As<std::string>();
    auto logicalSourceSchema = node["schema"];
    if (!logicalSourceSchema.IsSequence()) {
        throw ConfigurationException("schema should be a sequence");
    }
    auto schema = Schema::create();
    for (auto field = logicalSourceSchema.Begin(); field != logicalSourceSchema.End(); field++) {
        auto fieldName = (*field).first;
        schema->addField(fieldName, INT64);
    }
    return LogicalSource::create(logicalSourceName, schema);
}

}