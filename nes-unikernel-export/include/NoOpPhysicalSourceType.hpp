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

#ifndef NES_NOOPPHYSICALSOURCETYPE_HPP
#define NES_NOOPPHYSICALSOURCETYPE_HPP

#include <CLIOptions.h>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <YAMLModel.h>
#include <optional>

namespace NES {
class NoOpPhysicalSourceType : public PhysicalSourceType {
  public:
    NoOpPhysicalSourceType(std::string logicalSourceName,
                           std::string physicalSourceName,
                           SchemaType schemaType,
                           std::optional<TCPSourceConfiguration> tcpSourceConfiguration)
        : PhysicalSourceType(std::move(logicalSourceName), std::move(physicalSourceName), SourceType::NOOP_SOURCE),
          schemaType(schemaType), tcpSourceConfiguration(std::move(tcpSourceConfiguration)){};

    bool equal(const PhysicalSourceTypePtr& other) override { return other->getSourceType() == SourceType::NOOP_SOURCE; }

    std::string toString() override { return "NoOpPhysicalSourceType"; }

    void reset() override {
        //NoOp
    }
    SchemaType schemaType;
    std::optional<TCPSourceConfiguration> tcpSourceConfiguration;

  public:
    [[nodiscard]] std::optional<TCPSourceConfiguration> getTCP() const { return tcpSourceConfiguration; }
    [[nodiscard]] SchemaType getSchemaType() const { return schemaType; }
};
}// namespace NES

#endif//NES_NOOPPHYSICALSOURCETYPE_HPP
