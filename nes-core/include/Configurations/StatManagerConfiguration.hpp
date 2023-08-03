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

#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP
#define NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/details/EnumOptionDetails.hpp>
#include <ctime>
#include <string>

namespace NES::Configurations {

  class StatManagerConfig : public BaseConfiguration {
  public:
    StatManagerConfig() : BaseConfiguration("StatManagerConfig", "Configuration options for StatManager") {}

    StringOption physicalSourceName = {STATISTICS_CONFIG_PHYSICAL_SOURCE_NAME, "defaultPhysicalSourceName", "Physical source name."};
    StringOption field = {STATISTICS_CONFIG_FIELD, "defaultFieldName", "The name of the field on which the stat is generated."};
    StringOption statMethodName = {STATISTICS_CONFIG_STAT_METHOD_NAME, "defaultStatMethodName", "The name and the method concatenated."};
    UIntOption duration = {STATISTICS_CONFIG_DURATION, 1, "Duration of how long to generate statistics for in seconds."};
    UIntOption frequency = {STATISTICS_CONFIG_FREQUENCY, 100, "Frequency of how often/in which intervals statistics should be generated in seconds."};
    DoubleOption error = {STATISTICS_CONFIG_ERROR, 0.0001, "The parametrized error of a statistics object."};
    DoubleOption probability = {STATISTICS_CONFIG_PROBABILITY, 0.0001, "The probability that the error of the statistics object surpasses the parametrized error."};
    UIntOption depth = {STATISTICS_CONFIG_DEPTH, 0, "The depth of a statistics object."}; // note: the standard is only to set the error and probability. Only error and probability or depth and width should be set at once, but not all four together.
    UIntOption width = {STATISTICS_CONFIG_WIDTH, 0, "The width of a statistics object."};

    std::vector<Configurations::BaseOption*> getOptions() override {
      return {&physicalSourceName,
              &field,
              &statMethodName,
              &duration,
              &frequency,
              &error,
              &probability,
              &depth,
              &width};
    }
  };

} // namespace NES

#endif //NES_CORE_INCLUDE_CONFIGURATIONS_STATMANAGERCONFIGURATION_HPP
