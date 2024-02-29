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
#ifndef NES_CONFIGURATIONVALIDATION_H
#define NES_CONFIGURATIONVALIDATION_H

namespace NES::Configurations {

/**
 * @brief This class provides a general implementation for validation of configurations options.
 * @tparam T of the value.
 */
template<class T>
class ConfigurationValidation {
  public:

    virtual ~ConfigurationValidation() = default;
    /**
     * @brief Method to check the validity of a configuration option
     * @param configuration option
     * @return success if validated
     */
    virtual bool isValid(const T&) const = 0;
};
}
#endif//NES_CONFIGURATIONVALIDATION_H
