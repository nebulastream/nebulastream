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

#pragma once

#include <string>
#include <Configurations/Validation/ConfigurationValidation.hpp>

namespace NES::Configurations
{

/**
        * @brief This class implements validation for parameters that should represent non-negative floats
        */
class FloatValidation : public ConfigurationValidation
{
public:
    /**
             * @brief Method to check the validity of a parameter as a non-negative float
             * @param number number to validate
             * @return true if the parameter is a valid non-negative float, false otherwise
             */
    bool isValid(const std::string& number) const override;
};
}
