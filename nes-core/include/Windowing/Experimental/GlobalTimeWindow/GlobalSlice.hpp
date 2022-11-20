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
#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALSLICE_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALSLICE_HPP_
#include <cinttypes>
#include <memory>
#include <ostream>
#include <stdlib.h>
namespace NES::Windowing::Experimental {

/**
 * @brief State, to represent one or more aggregation values.
 * This is a wrapper around a small chuck of memory, which is dynamically allocated.
 */
class State {
  public:
    // Align memory chunk to STATE_ALIGNMENT
    static constexpr uint64_t STATE_ALIGNMENT = 8;

    /**
     * @brief Create a new state element, with a specific state size.
     * As this represents a single aggregation value, it will result in a small dynamic allocation.
     * @param stateSize
     */
    State(uint64_t stateSize);

    /**
     * @brief Resets the value of isInitialized
     */
    void reset();

    /**
     * @brief Destructor for the state, which frees the memory chuck.
     */
    ~State();
    const uint64_t stateSize;
    alignas(STATE_ALIGNMENT) void* ptr;
    bool isInitialized = false;
};

/**
 * @brief A global slice that contains key value pairs for a specific interval of [start, end).
 * The aggregate value is stored in the State object.
 */
class GlobalSlice {
  public:
    /**
     * @brief Constructor to create a new slice that covers a specific range between stat and end.
     * @param entrySize entry size of the content of a slice
     * @param start of the slice
     * @param end of the slice
     * @param index of the slice (currently we assume that we can calculate a slice index, to which a specific stream event is assigned).
     */
    GlobalSlice(uint64_t entrySize, uint64_t start, uint64_t end);

    /**
     * @brief Copy constructor to create a new slice
     * @param slice
     */
    GlobalSlice(GlobalSlice& slice);

    /**
     * @brief Constructor to create a uninitialized slice.
     * @param entrySize entry size of the content of a slice
     */
    GlobalSlice(uint64_t entrySize);

    /**
     * @brief Start of the slice.
     * @return uint64_t
     */
    inline uint64_t getStart() const { return start; }

    /**
     * @brief End of the slice.
     * @return uint64_t
     */
    inline uint64_t getEnd() const { return end; }

    /**
     * @brief Checks if a slice covers a specific ts.
     * A slice cover a cover a range from [startTs, endTs - 1]
     * @param ts
     * @return
     */
    inline bool coversTs(uint64_t ts) const { return start <= ts && end > ts; }

    /**
     * @brief State of the slice.
     * @return uint64_t
     */
    inline std::unique_ptr<State>& getState() { return state; }

    /**
     * @brief Reinitialize slice.
     * @param start
     * @param end
     */
    void reset(uint64_t start, uint64_t end);

  private:
    uint64_t start;
    uint64_t end;
    std::unique_ptr<State> state;
};

}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEWINDOW_GLOBALSLICE_HPP_
