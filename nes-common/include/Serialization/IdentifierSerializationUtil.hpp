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
#include <Identifiers/Identifier.hpp>

namespace NES
{
class SerializableIdentifierList;
}
namespace NES
{
class SerializableIdentifier;
}
namespace NES
{

class IdentifierSerializationUtil
{
public:
    static SerializableIdentifier* serializeIdentifier(const Identifier& identifier, SerializableIdentifier* serializedIdentifier);
    static Identifier deserializeIdentifier(const SerializableIdentifier& identifier);

    static SerializableIdentifierList* serializeIdentifierList(const IdentifierList& identifierList, SerializableIdentifierList* serializedIdentifierList);
    static IdentifierList deserializeIdentifierList(const SerializableIdentifierList& identifierList);
};
}