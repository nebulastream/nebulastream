// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//         https://www.apache.org/licenses/LICENSE-2.0
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// *


#ifndef NES_LORAWANPROXYSOURCEDESCRIPTOR_HPP
#define NES_LORAWANPROXYSOURCEDESCRIPTOR_HPP

#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
namespace NES {

class LoRaWANProxySourceDescriptor: public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, LoRaWANProxySourceTypePtr loRaWanProxySourceType, std::string logicalSourceName, std::string physicalSourceName);
    static SourceDescriptorPtr create(SchemaPtr schema, LoRaWANProxySourceTypePtr loRaWanProxySourceType);
    /**
     * @brief get source config ptr with all configurations for LoRaWANProxySource
     */
    LoRaWANProxySourceTypePtr getSourceConfig() const;

    std::string toString() override;
    bool equal(const SourceDescriptorPtr& other) override;
    SourceDescriptorPtr copy() override;

  private:
    explicit LoRaWANProxySourceDescriptor(
        SchemaPtr schema,
        LoRaWANProxySourceTypePtr sourceConfig,
        std::string logicalSourceName,
        std::string physicalSourceName);

    LoRaWANProxySourceTypePtr loRaWanProxySourceType;
};

}

#endif//NES_LORAWANPROXYSOURCEDESCRIPTOR_HPP
