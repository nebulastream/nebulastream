#ifndef NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_EVENTS_HPP_
#define NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_EVENTS_HPP_

namespace NES::Windowing::Experimental {
class SliceMergeTask {
  public:
    uint64_t sequenceNumber;
    uint64_t sliceEnd;
};

class WindowTriggerTask {
  public:
    uint64_t sequenceNumber;
    uint64_t startSlice;
    uint64_t endSlice;
};

}

#endif//NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_EVENTS_HPP_
