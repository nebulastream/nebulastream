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
#ifndef NES_STORAGERESOURCELOCKER_HPP
#define NES_STORAGERESOURCELOCKER_HPP
#include <cstdint>
#include <future>
#include <vector>
#include <Identifiers.hpp>

namespace NES::RequestProcessor {
using RequestId = uint64_t;
enum class ResourceType : uint8_t;
class StorageHandler;
using StorageHandlerPtr = std::shared_ptr<StorageHandler>;

class StorageResourceLocker {
  public:
    /**
     * @brief set the id of this request. This has to be done before the request is executed.
     * @param requestId
     */
    void setId(RequestId requestId);
  private:
    std::vector<ResourceType> requiredResources;
  protected:
    StorageResourceLocker(std::vector<ResourceType> requiredResources);

    /**
     * @brief Performs steps to be done before execution of the request logic, e.g. locking the required data structures
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void preExecution(const StorageHandlerPtr& storageHandle);
    /**
     * @brief Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param storageHandle: The storage access handle used by the request
     */
    virtual void postExecution(const StorageHandlerPtr& storageHandle);

    RequestId requestId{INVALID_REQUEST_ID};
};
}
#endif//NES_STORAGERESOURCELOCKER_HPP
