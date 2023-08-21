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
#include <Exceptions/AccessNonLockedResourceException.hpp>
namespace NES::Exceptions {
AccessNonLockedResourceException::AccessNonLockedResourceException(const std::string& message, RequestProcessor::Experimental::ResourceType resourceType)
    : RequestExecutionException(message), resourceType(resourceType) {}

RequestProcessor::Experimental::ResourceType AccessNonLockedResourceException::getResourceType() { return resourceType; }

}// namespace NES::Exceptions
