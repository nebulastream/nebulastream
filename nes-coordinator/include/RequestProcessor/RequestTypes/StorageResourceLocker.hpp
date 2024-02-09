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
#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_STORAGERESOURCELOCKER_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_STORAGERESOURCELOCKER_HPP_
#include <Identifiers.hpp>
#include <cstdint>
#include <future>
#include <vector>

namespace NES::RequestProcessor {
using RequestId = uint64_t;
enum class ResourceType : uint8_t;
class StorageHandler;
using StorageHandlerPtr = std::shared_ptr<StorageHandler>;

//todo #4433: add template parameter, move execute functions from subclasses here and rename this class accordingly
/**
 * This is the common base class of objects that use a storage handler to lock resources before manipulating them
 */
class StorageResourceLocker {
  protected:
    /**
     * @brief Constructor
     * @param requiredResources a list of the resources to be locked
     */
    explicit StorageResourceLocker(std::vector<ResourceType> requiredResources);

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

    /**
     * @brief get an id that identifies this object to lock resources for exclusive use by this object
     * @return the id
     */
    virtual RequestId getResourceLockingId() = 0;

  private:
    std::vector<ResourceType> requiredResources;
};
}// namespace NES::RequestProcessor
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_STORAGERESOURCELOCKER_HPP_
