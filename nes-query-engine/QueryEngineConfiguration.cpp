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

#include <charconv>
#include <cstddef>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <Configurations/Validation/ConfigurationValidation.hpp>
#include <Util/Logger/Logger.hpp>
#include <QueryEngineConfiguration.hpp>
#include <magic_enum.hpp>

std::shared_ptr<NES::Configurations::ConfigurationValidation> NES::Runtime::QueryEngineConfiguration::numberOfThreadsValidator()
{
    struct Validator : Configurations::ConfigurationValidation
    {
        [[nodiscard]] bool isValid(const std::string& stringValue) const override
        {
            size_t value{};
            auto [ptr, ec] = std::from_chars(stringValue.data(), stringValue.data() + stringValue.size(), value);
            if (ec != std::errc{})
            {
                NES_ERROR(
                    "Invalid WorkerThread configuration: {}. Error during parsing: {}", stringValue.data(), magic_enum::enum_name(ec));
                return false;
            }

            if (value == 0)
            {
                NES_ERROR("Invalid WorkerThread configuration: {}. Cannot be zero", stringValue.data());
                return false;
            }

            if (value > std::thread::hardware_concurrency())
            {
                NES_ERROR(
                    "Cannot use more worker thread than available cpus: {} vs. {} available CPUs",
                    value,
                    std::thread::hardware_concurrency());
                return false;
            }

            return true;
        }
    };

    return std::make_shared<Validator>();
}

std::shared_ptr<NES::Configurations::ConfigurationValidation> NES::Runtime::QueryEngineConfiguration::taskQueueSizeValidator()
{
    struct Validator : Configurations::ConfigurationValidation
    {
        [[nodiscard]] bool isValid(const std::string& stringValue) const override
        {
            size_t value{};
            auto [ptr, ec] = std::from_chars(stringValue.data(), stringValue.data() + stringValue.size(), value);
            if (ec != std::errc{})
            {
                NES_ERROR(
                    "Invalid TaskQueueSize configuration: {}. Error during parsing: {}", stringValue.data(), magic_enum::enum_name(ec));
                return false;
            }
            return true;
        }
    };

    return std::make_shared<Validator>();
}
