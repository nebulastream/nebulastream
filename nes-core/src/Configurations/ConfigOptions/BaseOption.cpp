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

#include <Configurations/ConfigOptions/BaseOption.hpp>

namespace NES::Configurations {

BaseOption::BaseOption(const std::string& name, const std::string& description) : name(name), description(description){};

bool BaseOption::operator==(const BaseOption& other) { return name == other.name && description == other.description; };

std::string BaseOption::getName() { return name; }

std::string BaseOption::getDescription() { return description; }

}// namespace NES::Configurations