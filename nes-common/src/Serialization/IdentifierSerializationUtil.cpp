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

#include <utility>
#include <vector>
#include <Serialization/IdentifierSerializationUtil.hpp>

#include <SerializableVariantDescriptor.pb.h>
#include <SerializableIdentifier.pb.h>

NES::SerializableIdentifier*
NES::IdentifierSerializationUtil::serializeIdentifier(const Identifier& identifier, SerializableIdentifier* serializedIdentifier)
{
    serializedIdentifier->set_casesensitive(identifier.isCaseSensitive());
    serializedIdentifier->set_value(identifier.getOriginalString());
    return serializedIdentifier;
}

NES::Identifier NES::IdentifierSerializationUtil::deserializeIdentifier(const SerializableIdentifier& identifier)
{
    return Identifier{identifier.value(), identifier.casesensitive()};
}

NES::SerializableIdentifierList* NES::IdentifierSerializationUtil::serializeIdentifierList(
    const IdentifierList& identifierList, SerializableIdentifierList* serializedIdentifierList)
{
    for (const auto& identifier : identifierList)
    {
        auto* serializedIdentifier = serializedIdentifierList->add_identifiers();
        serializeIdentifier(identifier, serializedIdentifier);
    }
    return serializedIdentifierList;
}

NES::IdentifierList NES::IdentifierSerializationUtil::deserializeIdentifierList(const SerializableIdentifierList& identifierList)
{
    std::vector<Identifier> identifierListCopy;
    for (const auto& serializedIdentifier : identifierList.identifiers())
    {
        identifierListCopy.push_back(deserializeIdentifier(serializedIdentifier));
    }
    return IdentifierList{std::move(identifierListCopy)};
}
