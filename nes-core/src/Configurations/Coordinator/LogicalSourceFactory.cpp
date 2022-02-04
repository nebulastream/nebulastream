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