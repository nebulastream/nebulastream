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

#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_ELEGANTCONFIGURATIONS_HPP_
#define NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_ELEGANTCONFIGURATIONS_HPP_

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <memory>

namespace NES::Configurations {

/**
 * Define all ELEGANT related configuration parameters
 */
class ElegantConfigurations : public BaseConfiguration {

  public:
    ElegantConfigurations() : BaseConfiguration(){};

    ElegantConfigurations(const std::string& name, const std::string& description) : BaseConfiguration(name, description){};

    /**
     * @brief Accelerate java UDFs.
     */
    BoolOption accelerateJavaUDFs = {ACCELERATE_JAVA_UDFS, false, "Accelerate java UDFs."};

    /**
     * @brief ELEGANT external planner service URL. Example: https://localhost:8081/plan
     */
    StringOption plannerServiceURL = {
            PLANNER_SERVICE_URL,
            "https://localhost:8081/planner/schedule",
            "Complete URL for connecting with the ELEGANT planner."
    };

    /**
     * @brief ELEGANT external planner service URL. Example: https://localhost:8081/plan
     */
    StringOption accelerationServiceURL = {ACCELERATION_SERVICE_URL, "Complete URL for connecting with the ELEGANT planner."};

    /**
     * @brief ELEGANT weight between performance (time) and energy efficiency. Example: time_weight = 0.95,
     * energy_weight = (1 - time_weight) = 0.05
     */
    FloatOption timeWeight = {
            TIME_WEIGHT,
            0.5,
            "Weight (range of 0-1) for Optimizer to choose between time and energy."
    };

  private:
    std::vector<Configurations::BaseOption*> getOptions() override { return {&accelerateJavaUDFs}; }
};
}// namespace NES::Configurations
#endif// NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_ELEGANTCONFIGURATIONS_HPP_
