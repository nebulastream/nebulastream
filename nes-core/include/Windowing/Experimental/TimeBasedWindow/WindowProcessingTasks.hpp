#ifndef NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_WINDOWPROCESSINGTASKS_HPP_
#define NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_WINDOWPROCESSINGTASKS_HPP_

namespace NES::Windowing::Experimental {

/**
 * @brief This task models the merge task of an a specific slice, with a start and a end.
 */
class SliceMergeTask {
  public:
    uint64_t sequenceNumber;
    uint64_t startSlice;
    uint64_t endSlice;
};


/**
 * @brief This task models the trigger of a window with a start and a end.
 */
class WindowTriggerTask {
  public:
    uint64_t sequenceNumber;
    uint64_t windowStart;
    uint64_t windowEnd;
};

}

#endif//NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_WINDOWPROCESSINGTASKS_HPP_
