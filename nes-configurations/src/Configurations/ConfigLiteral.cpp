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

#include <Configurations/ConfigLiteral.hpp>

#include <cstddef>
#include <functional>
#include <ostream>
#include <utility>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Util/Hash.hpp>
#include <folly/hash/Hash.h>

namespace NES
{
LiteralConfigValue::LiteralConfigValue(QualifiedIdentifier name, ConfigLiteral value) : name(std::move(name)), value(std::move(value))
{
}

const QualifiedIdentifier& LiteralConfigValue::getFullyQualifiedName() const
{
    return name;
}

const ConfigLiteral& LiteralConfigValue::getValue() const
{
    return value;
}

bool operator==(const LiteralConfigValue& lhs, const LiteralConfigValue& rhs) = default;

std::ostream& operator<<(std::ostream& os, const LiteralConfigValue& value)
{
    return os << value.name;
}

}

std::size_t std::hash<NES::LiteralConfigValue>::operator()(const NES::LiteralConfigValue& value) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, value.getFullyQualifiedName(), value.getValue());
}
