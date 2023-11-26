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
#ifndef NES_COORDINATOR_INCLUDE_EXCEPTIONS_ACCESSNONLOCKEDRESOURCEEXCEPTION_HPP_
#define NES_COORDINATOR_INCLUDE_EXCEPTIONS_ACCESSNONLOCKEDRESOURCEEXCEPTION_HPP_
#include <Exceptions/RequestExecutionException.hpp>
namespace NES {
namespace RequestProcessor {
enum class ResourceType : uint8_t;
}
namespace Exceptions {
/**
 * @brief This exception is raised, in case a request tries to access a resource via the storage handler without having locked it before.
 */
class AccessNonLockedResourceException : public RequestExecutionException {
  public:
    explicit AccessNonLockedResourceException(const std::string& message,
                                              RequestProcessor::ResourceType resourceType);

    /**
     * @brief Access the type of the resource which could not be accessed
     * @return the resource type
     */
    RequestProcessor::ResourceType getResourceType();

  private:
    RequestProcessor::ResourceType resourceType;
};
}// namespace Exceptions
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_EXCEPTIONS_ACCESSNONLOCKEDRESOURCEEXCEPTION_HPP_
