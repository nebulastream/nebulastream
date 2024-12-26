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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_OPERATORHANDLERSTORE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_OPERATORHANDLERSTORE_HPP_
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <folly/Synchronized.h>
#include <unordered_map>

namespace NES {
using OperatorHash = std::tuple<SharedQueryId, DecomposedQueryId, OperatorId>;
class OperatorHandlerStore;
using OperatorHandlerStorePtr = std::shared_ptr<OperatorHandlerStore>;

class OperatorHandlerStore {
  public:
    static OperatorHandlerStorePtr create();

    /**
     * @brief Check that storage contains operator handler by key
     * @param queryId query id
     * @param planId decomposed query plan id
     * @param operatorId operator id
     * @return true if operator handler stored
     */
    bool contains(const SharedQueryId queryId, const DecomposedQueryId planId, const OperatorId operatorId);

    /**
     * @brief Return operator handler that storage contains by key
     * @param queryId query id
     * @param planId decomposed query plan id
     * @param operatorId operator id
     * @return operator handler
     */
    Runtime::Execution::OperatorHandlerPtr
    getOperatorHandler(const SharedQueryId queryId, const DecomposedQueryId planId, const OperatorId operatorId);

    /**
     * @brief Store operator handler to storage  by key
     * @param queryId query id
     * @param planId decomposed query plan id
     * @param operatorId operator id
     * @param handler operator handler that needs to be stored
     * @return operator handler
     */
    void storeOperatorHandler(const SharedQueryId queryId,
                              const DecomposedQueryId planId,
                              const OperatorId operatorId,
                              Runtime::Execution::OperatorHandlerPtr handler);

    /**
     * @brief Remove all operator handlers for query
     * @param queryId query id
     */
    void removeOperatorHandlers(const SharedQueryId queryId);

    /**
     * @brief Remove all operator handlers for query and decomposed query plan
     * @param queryId query id
     * @param planId decomposed query plan id
     */
    void removeOperatorHandlers(const SharedQueryId queryId, const DecomposedQueryId planId);

  private:
    folly::Synchronized<std::unordered_map<OperatorHash, Runtime::Execution::OperatorHandlerPtr>> operatorHandlerStorage;
};
}// namespace NES

#endif// NES_RUNTIME_INCLUDE_RUNTIME_OPERATORHANDLERSTORE_HPP_
