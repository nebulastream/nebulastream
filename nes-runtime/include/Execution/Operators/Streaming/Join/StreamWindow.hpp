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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMWINDOW_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMWINDOW_HPP_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <vector>

namespace NES::Runtime::Execution {

/**
 * @brief This class represents a single window for a join. It stores all values for the left and right stream.
 */
class StreamWindow {
  public:
    enum class WindowState : uint8_t { BOTH_SIDES_FILLING, EMITTED_TO_PROBE, ONCE_SEEN_DURING_TERMINATION };

    /**
     * @brief Constructor for creating a window
     * @param windowStart: Start timestamp of this window
     * @param windowEnd: End timestamp of this window
     */
    explicit StreamWindow(uint64_t windowStart, uint64_t windowEnd);

    /**
     * @brief Compares if two windows are equal
     * @param rhs
     * @return Boolean
     */
    bool operator==(const StreamWindow& rhs) const;

    /**
     * @brief Compares if two windows are NOT equal
     * @param rhs
     * @return Boolean
     */
    bool operator!=(const StreamWindow& rhs) const;

    /**
     * @brief Getter for the start ts of the window
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getWindowStart() const;

    /**
     * @brief Getter for the end ts of the window
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getWindowEnd() const;

    /**
     * @brief Check if this window has been emitted to the probe
     * @return Boolean
     */
    bool isAlreadyEmitted() const;

    /**
     * @brief Returns the number of tuples in this window for the left side
     * @return uint64_t
     */
    virtual uint64_t getNumberOfTuplesLeft() = 0;

    /**
     * @brief Returns the number of tuples in this window for the right side
     * @return uint64_t
     */
    virtual uint64_t getNumberOfTuplesRight() = 0;

    /**
     * @brief Returns the identifier for this window. For now, the identifier is the windowEnd
     * @return uint64_t
     */
    uint64_t getWindowIdentifier() const;

    /**
     * @brief Wrapper for std::atomic<T>::compare_exchange_strong
     * @param expectedState
     * @param newWindowState
     * @return Bool
     */
    bool compareExchangeStrong(WindowState expectedState, WindowState newWindowState);

    /**
     * @brief Checks if this window should be triggered during termination. During query termination, we should only trigger a
     * window if it has been seen from all sides (left and right)
     * @return
     */
    bool shouldTriggerDuringTerminate();

    /**
     * @brief Creates a string representation of this window
     * @return String
     */
    virtual std::string toString() = 0;

    /**
     * @brief Virtual deconstructor
     */
    virtual ~StreamWindow() = default;

  protected:
    std::atomic<WindowState> windowState;
    uint64_t windowStart;
    uint64_t windowEnd;
    std::mutex triggerTerminationMutex;
};

using StreamWindowPtr = std::shared_ptr<StreamWindow>;

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMWINDOW_HPP_
