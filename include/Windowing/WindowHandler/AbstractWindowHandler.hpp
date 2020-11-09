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

#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <Util/Logger.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>

namespace NES::Windowing {

/**
 * @brief The abstract window handler is the base class for all window handlers
 */
class AbstractWindowHandler : public std::enable_shared_from_this<AbstractWindowHandler> {
  public:
    template<class Type>
    auto as() {
        return std::dynamic_pointer_cast<Type>(shared_from_this());
    }

    /**
    * @brief Starts thread to check if the window should be triggered.
    * @return boolean if the window thread is started
    */
    virtual bool start() = 0;

    /**
     * @brief Stops the window thread.
     * @return
     */
    virtual bool stop() = 0;

    /**
     * @brief triggers all ready windows.
     * @return
     */
    virtual void trigger() = 0;

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    virtual void updateAllMaxTs(uint64_t ts, uint64_t originId) = 0;

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    virtual bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager, PipelineStagePtr nextPipeline, uint32_t pipelineStageId, uint64_t originId) = 0;

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    virtual WindowManagerPtr getWindowManager() = 0;

    virtual std::string toString() = 0;

    virtual LogicalWindowDefinitionPtr getWindowDefinition() = 0;

  protected:
    std::atomic_bool running{false};
    WindowManagerPtr windowManager;
    PipelineStagePtr nextPipeline;
    uint32_t pipelineStageId;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    uint64_t originId;
};

}// namespace NES::Windowing
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
