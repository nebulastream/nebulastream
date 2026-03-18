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

#include <Identifiers/QualifiedIdentifier.hpp>

#include <span>
#include <vector>
#include <Identifiers/Identifier.hpp>

namespace NES
{

QualifiedIdentifier
Unreflector<QualifiedIdentifierBase<std::dynamic_extent>>::operator()(const Reflected& reflectable, const ReflectionContext& context) const
{
    /// Mirror of Reflector<QualifiedIdentifierBase>: each Identifier is unreflected via Unreflector<Identifier>,
    /// which restores the case-sensitivity flag from the serialized representation.
    return QualifiedIdentifier{context.unreflect<std::vector<Identifier>>(reflectable)};
}
}
