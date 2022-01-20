/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_
#define NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_

#include <Configurations/ConfigurationOption.hpp>
#include <map>
#include <memory>
#include <string>

namespace NES {

enum SourceType {
    OPC_SOURCE,
    ZMQ_SOURCE,
    CSV_SOURCE,
    KAFKA_SOURCE,
    TEST_SOURCE,
    BINARY_SOURCE,
    SENSE_SOURCE,
    DEFAULT_SOURCE,
    NETWORK_SOURCE,
    ADAPTIVE_SOURCE,
    MONITORING_SOURCE,
    YSB_SOURCE,
    MEMORY_SOURCE,
    MQTT_SOURCE,
    LAMBDA_SOURCE,
    BENCHMARK_SOURCE
};

/**
 * enum string mapping for source type
 */
static std::map<std::string, SourceType> stringToSourceType{{Configurations::SENSE_SOURCE_CONFIG, SENSE_SOURCE},
                                                            {Configurations::CSV_SOURCE_CONFIG, CSV_SOURCE},
                                                            {Configurations::BINARY_SOURCE_CONFIG, BINARY_SOURCE},
                                                            {Configurations::MQTT_SOURCE_CONFIG, MQTT_SOURCE},
                                                            {Configurations::KAFKA_SOURCE_CONFIG, KAFKA_SOURCE},
                                                            {Configurations::OPC_SOURCE_CONFIG, OPC_SOURCE},
                                                            {Configurations::DEFAULT_SOURCE_CONFIG, DEFAULT_SOURCE}};

/**
 * enum source type to string
 */
static std::map<SourceType, std::string> sourceTypeToString{{SENSE_SOURCE, Configurations::SENSE_SOURCE_CONFIG},
                                                            {CSV_SOURCE, Configurations::CSV_SOURCE_CONFIG},
                                                            {BINARY_SOURCE, Configurations::BINARY_SOURCE_CONFIG},
                                                            {MQTT_SOURCE, Configurations::MQTT_SOURCE_CONFIG},
                                                            {KAFKA_SOURCE, Configurations::KAFKA_SOURCE_CONFIG},
                                                            {OPC_SOURCE, Configurations::OPC_SOURCE_CONFIG},
                                                            {DEFAULT_SOURCE, Configurations::DEFAULT_SOURCE_CONFIG}};

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

/**
 * @brief Interface for different Physical Source types
 */
class PhysicalSourceType : public std::enable_shared_from_this<PhysicalSourceType> {

  public:
    PhysicalSourceType(SourceType sourceType);

    ~PhysicalSourceType() noexcept = default;

    /**
     * Checks equality
     * @param other mqttSourceType ot check equality for
     * @return true if equal, false otherwise
     */
    virtual bool equal(PhysicalSourceTypePtr const& other) = 0;

    /**
     * @brief creates a string representation of the source
     * @return
     */
    virtual std::string toString() = 0;

    /**
     * @brief Return source type
     * @return enum representing source type
     */
    SourceType getSourceType();

    /**
     * @brief Return source type
     * @return string representing source type
     */
    std::string getSourceTypeAsString();

    /**
     * @brief reset the values to default
     */
    virtual void reset()=0;

    /**
     * @brief Checks if the current Source is of a specific Source Type
     * @tparam PhysicalSourceType: the source type to check
     * @return bool true if Source is of PhysicalSourceType
     */
    template<class SourceType>
    bool instanceOf() {
        if (dynamic_cast<SourceType*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the PhysicalSourceType to a specific Source Type
    * @tparam SourceType: source type to cast to
    * @return returns a shared pointer of the PhysicalSourceType
    */
    template<class SourceType>
    std::shared_ptr<SourceType> as() {
        if (instanceOf<SourceType>()) {
            return std::dynamic_pointer_cast<SourceType>(this->shared_from_this());
        }
        throw std::logic_error("Node:: we performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(SourceType).name());
    }

  private:
    SourceType sourceType;
};

}// namespace NES
#endif// NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_
