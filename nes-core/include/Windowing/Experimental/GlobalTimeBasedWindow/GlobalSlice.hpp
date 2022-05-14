#ifndef NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLICE_HPP_
#define NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLICE_HPP_
#include <cinttypes>
#include <memory>
#include <ostream>
#include <stdlib.h>
namespace NES::Windowing::Experimental {

class State {
  public:
    static constexpr uint64_t STATE_ALIGNMENT = 8;
    State(uint64_t stateSize);
    void reset();
    ~State();
    const uint64_t stateSize;
    alignas(STATE_ALIGNMENT) void* ptr;
};

/**
 * @brief A global slice that contains key value pairs for a specific interval of [start, end[.
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
     * @brief Constructor to create a uninitialized slice.
     * @param entrySize entry size of the content of a slice
     */
    GlobalSlice(uint64_t entrySize);
    GlobalSlice(GlobalSlice& entrySize);

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

#endif//NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALTIMEBASEDWINDOW_GLOBALSLICE_HPP_
