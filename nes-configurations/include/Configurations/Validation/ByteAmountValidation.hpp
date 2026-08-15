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

#include <cstdint>
#include <string>
#include <ostream>
#include <Configurations/Validation/ConfigurationValidation.hpp>
#include <yaml-cpp/yaml.h>

namespace NES
{

/// @brief Struct representing an amount of bytes.
struct Byte {
    uint64_t value;

    Byte() : value(0) {}
    explicit Byte(uint64_t value) : value(value) {}

    bool operator==(const Byte& other) const {
        return value == other.value;
    }

    bool operator!=(const Byte& other) const {
        return value != other.value;
    }
};

inline std::ostream& operator<<(std::ostream& os, const Byte& b) {
    os << b.value;
    return os;
}

/// @brief Parser function to convert a resource string to Byte
/// Supports Kubernetes' resource.Quantity grammar.
Byte parseByteAmount(const std::string& byteAmountStr);

/// @brief This class implements validation for parameters that should represent a byte amount
class ByteAmountValidation : public ConfigurationValidation
{
public:
    /// @brief Method to check the validity of a parameter as a byte amount
    bool isValid(const std::string& parameter) const override;
};
}

namespace YAML {
template<>
struct convert<NES::Byte> {
  static Node encode(const NES::Byte& rhs) {
    Node node;
    node = rhs.value;
    return node;
  }

  static bool decode(const Node& node, NES::Byte& rhs) {
    if (!node.IsScalar()) {
      return false;
    }
    try {
        rhs = NES::parseByteAmount(node.as<std::string>());
        return true;
    } catch (...) {
        return false;
    }
  }
};
}
