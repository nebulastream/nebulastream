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

#include <Identifiers/Identifier.hpp>

#include <span>
#include <utility>
#include <variant>
#include <ErrorHandling.hpp>
#include <nameof.hpp>

namespace NES
{

IdentifierList
Unreflector<IdentifierListBase<std::dynamic_extent>>::operator()(const Reflected& reflectable, const ReflectionContext&) const
{
    if (auto stringResult = reflectable->to_string(); stringResult.has_value())
    {
        auto idListExp = IdentifierList::tryParse(stringResult.value());
        if (!idListExp.has_value())
        {
            throw std::move(idListExp).error();
        }
        return idListExp.value();
    }
    throw CannotDeserialize(
        "Expected string for IdentifierList, but got {} instead",
        std::visit([](const auto& actual) { return NAMEOF_TYPE(decltype(actual)); }, reflectable->get()));
}
}
