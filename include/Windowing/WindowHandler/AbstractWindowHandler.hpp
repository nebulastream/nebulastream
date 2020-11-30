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
#include <algorithm>
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
    * @brief Initialises the state of this window depending on the window definition.
    */
    virtual bool setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager, PipelineStagePtr nextPipeline,
                       uint32_t pipelineStageId, uint64_t originId) = 0;

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    virtual WindowManagerPtr getWindowManager() = 0;

    virtual std::string toString() = 0;

    virtual LogicalWindowDefinitionPtr getWindowDefinition() = 0;

    /**
     * @brief Gets the last processed watermark
     * @return watermark
     */
    [[nodiscard]] uint64_t getLastWatermark() const { return lastWatermark; }

    /**
     * @brief Sets the last watermark
     * @param lastWatermark
     */
    void setLastWatermark(uint64_t lastWatermark) { this->lastWatermark = lastWatermark; }

    /**
     * @brief Gets the maximal processed ts per origin id.
     * @param originId
     * @return max ts.
     */
    uint32_t getMaxTs(uint32_t originId) { return originIdToMaxTsMap[originId]; };

    /**
     * @brief Gets number of mappings.
     * @return size of origin map.
     */
    uint64_t getNumberOfMappings() { return originIdToMaxTsMap.size(); };

    /**
     * @brief this function prints the content of the map for debugging purposes
     * @return
     */
    std::string getAllMaxTs() {
        std::stringstream ss;
        for (auto& a : originIdToMaxTsMap) {
            ss << " id=" << a.first << " val=" << a.second;
        }
        return ss.str();
    }

    /**
     * @brief Calculate the min watermark
     * @return MinAggregationDescriptor watermark
     */
    uint64_t getMinWatermark() {
        if (originIdToMaxTsMap.size() == numberOfInputEdges) {
            std::map<uint64_t, uint64_t>::iterator min =
                std::min_element(originIdToMaxTsMap.begin(), originIdToMaxTsMap.end(),
                                 [](const std::pair<uint64_t, uint64_t>& a, const std::pair<uint64_t, uint64_t>& b) -> bool {
                                     return a.second < b.second;
                                 });

            std::stringstream ss;
            for (auto& entry : originIdToMaxTsMap) {
                ss << " id=" << entry.first << " max=" << entry.second;
            }
            NES_DEBUG("map=" << ss.str());
            NES_DEBUG("getMinWatermark() return min=" << min->second);
            return min->second;
        } else {
            NES_DEBUG("getMinWatermark() return 0 because there is no mapping yet current number of mappings="
                      << originIdToMaxTsMap.size() << " expected mappings=" << numberOfInputEdges);
            return 0;//TODO: we have to figure out how many downstream positions are there
        }
    };

    /**
     * @brief Update the max processed ts, per origin.
     * @param ts
     * @param originId
     */
    virtual void updateMaxTs(uint64_t ts, uint64_t originId) = 0;

  protected:
    std::atomic_bool running{false};
    WindowManagerPtr windowManager;
    PipelineStagePtr nextPipeline;
    uint32_t pipelineStageId;
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    uint64_t originId;
    std::map<uint64_t, uint64_t> originIdToMaxTsMap;
    uint64_t lastWatermark;
    uint64_t numberOfInputEdges;
};

}// namespace NES::Windowing
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
