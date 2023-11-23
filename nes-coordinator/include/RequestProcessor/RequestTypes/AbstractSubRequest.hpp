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
#ifndef NES_ABSTRACTSUBREQUEST_HPP
#define NES_ABSTRACTSUBREQUEST_HPP
#include <any>
#include <future>
#include <memory>
#include <vector>

namespace NES::RequestProcessor::Experimental {
class AbstractRequest;
using AbstractRequestPtr = std::shared_ptr<AbstractRequest>;

// class AbstractRequestResponse;
// using AbstractRequestResponsePtr = std::shared_ptr<AbstractRequestResponse>;

class StorageHandler;
using StorageHandlerPtr = std::shared_ptr<StorageHandler>;


class AbstractSubRequest {
public:
    /**
     * @brief destructor
     */
    virtual ~AbstractSubRequest() = default;
    virtual void execute(const StorageHandlerPtr& storageHandler) = 0;

    std::future<std::any> getFuture();
protected:
    std::promise<std::any> responsePromise;
};

}

#endif//NES_ABSTRACTSUBREQUEST_HPP
