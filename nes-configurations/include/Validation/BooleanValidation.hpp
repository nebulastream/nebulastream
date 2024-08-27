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

namespace NES::Configurations
{

/**
    * @brief This class implements validation for parameters that should represent booleans
    */
class BooleanValidation
{
public:
    /**
         * @brief Method to check the validity of a parameter as a bool
         * @param parameter number to validate
         * @return true if the parameter is a valid bool, false otherwise
         */
    static bool isValid (const std::string& parameter)
    {
        std::string lowerParam = parameter;
        std::transform(lowerParam.begin(), lowerParam.end(), lowerParam.begin(), [](unsigned char c) { return std::tolower(c); });

        return lowerParam == "true" || lowerParam == "false" || lowerParam == "1" || lowerParam == "0";
    }
};
} /// namespace NES::Configurations
