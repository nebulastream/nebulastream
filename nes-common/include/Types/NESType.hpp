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
#include "Identifiers/QualifiedIdentifier.hpp"
#include "Util/Any.hpp"

#include "NESType.hpp"

namespace NES
{

class NESValue;
class NamedNESValue;
class NESType
{
    QualifiedIdentifier name;
    std::function<NESValue(const Schema<NamedNESValue, Ordered>&)> factory;
};

class NESValue
{
    std::shared_ptr<NESType> type;
    ExplicitAny internalRepresentation;
};

class NESTypeClass
{
    QualifiedIdentifier name;
    std::function<bool(NESType)> predicate;

public:
    NESTypeClass(QualifiedIdentifier name, std::function<bool(NESType)> predicate) : name(std::move(name)), predicate(std::move(predicate)) { }

    bool validate(const NESType& type) const;
};
}
