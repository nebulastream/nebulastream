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

#include <cstddef>
#include <memory>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>

namespace NES::QueryCompilation
{
/**
 * @brief Represents a query compilation request.
 * The request encapsulates the decomposed query plan and addition properties.
 */
class QueryCompilationRequest
{
public:
    static std::shared_ptr<QueryCompilationRequest> create(std::shared_ptr<DecomposedQueryPlan> decomposedQueryPlan, size_t bufferSize);

    /**
     * @brief Enable debugging for this query.
     */
    void enableDebugging();

    /**
     * @brief Checks if debugging is enabled
     * @return bool
     */
    bool isDebugEnabled() const;

    /**
    * @brief Checks if optimizations for this query
    */
    void enableOptimizations();

    /**
     * @brief Checks if optimization flags is enabled
     * @return bool
     */
    bool isOptimizeEnabled() const;

    /**
    * @brief Enable debugging for this query.
    */
    void enableDump();

    /**
     * @brief Checks if dumping to nesviz is enabled
     * @return bool
     */
    bool isDumpEnabled() const;

    /**
     * @brief Gets the Decomposed query plan of this request
     * @return std::shared_ptr<DecomposedQueryPlan>
     */
    std::shared_ptr<DecomposedQueryPlan> getDecomposedQueryPlan();

    [[nodiscard]] size_t getBufferSize() const;
    void setBufferSize(size_t bufferSize);

private:
    explicit QueryCompilationRequest(std::shared_ptr<DecomposedQueryPlan> queryPlan, size_t bufferSize);
    std::shared_ptr<DecomposedQueryPlan> decomposedQueryPlan;
    bool debug;
    bool optimize;
    bool dumpQueryPlans;
    size_t bufferSize; /// TODO #403: Use QueryCompiler Configuration instead
};
}
