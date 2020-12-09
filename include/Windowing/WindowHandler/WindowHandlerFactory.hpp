/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_

#include <Windowing/JoinForwardRefs.hpp>
#include <memory>

namespace NES::Windowing {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

class AbstractWindowHandler;
typedef std::shared_ptr<AbstractWindowHandler> AbstractWindowHandlerPtr;

class WindowHandlerFactory {
  public:
    /**
     * @brief Creates a window handler for aggregations only by using the window definition.
     * @param windowDefinition window definition
     * @return AbstractWindowHandlerPtr
     */
    static AbstractWindowHandlerPtr createAggregationWindowHandler(LogicalWindowDefinitionPtr windowDefinition,
                                                                   SchemaPtr outputSchema);

    /**
   * @brief Creates a join handler
   * @param joinDefinition window definition
   * @return AbstractJoinHandlerPtr
   */
    static Join::AbstractJoinHandlerPtr createJoinWindowHandler(Join::LogicalJoinDefinitionPtr joinDefinition);
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
