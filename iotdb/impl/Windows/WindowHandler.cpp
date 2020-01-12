
#include <atomic>
#include <iostream>
#include <memory>
#include <Util/Logger.hpp>
#include <boost/serialization/export.hpp>
#include <State/StateManager.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <Windows/WindowHandler.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(NES::WindowHandler)
namespace NES {

WindowHandler::WindowHandler(NES::WindowDefinitionPtr window_definition_ptr) : window_definition_ptr(window_definition_ptr) {
}

void WindowHandler::setup() {
  // Initialize WindowHandler Manager
  this->window_manager_ptr = std::make_shared<WindowManager>(this->window_definition_ptr);
  // Initialize StateVariable
  this->window_state = &StateManager::instance().registerState<int64_t, WindowSliceStore<int64_t> *>("window");
}

template<class FinalAggregateType, class PartialAggregateType>
void WindowHandler::aggregateWindows(
    WindowSliceStore<PartialAggregateType> *store,
    WindowDefinitionPtr window_definition_ptr,
    TupleBufferPtr tuple_buffer) {

  // we use the maximal records ts as watermark.
  // TODO we should add a allowed lateness to support out of order events
  auto watermark = store->getMaxTs();

  // create result vector of windows
  auto windows = std::make_shared<std::vector<WindowState>>();
  // the window type addes result windows to the windows vectors
  window_definition_ptr->windowType->triggerWindows(windows, store->getLastWatermark(), watermark);
  // allocate partial final aggregates for each window
  auto partial_final_aggregates = std::vector<PartialAggregateType>(windows->size());
  // iterate over all slices and update the partial final aggregates
  auto slices = store->getSliceMetadata();
  auto partial_aggregates = store->getPartialAggregates();
  for (uint64_t s_id = 0; s_id < slices.size(); s_id++) {
    for (uint64_t w_id = 0; w_id < windows->size(); w_id++) {
      auto window = (*windows)[w_id];
      // A slice is contained in a window if the window starts before the slice and ends after the slice
      if (window.getStartTs() <= slices[s_id].getStartTs() &&
          window.getEndTs() >= slices[s_id].getEndTs()) {
        //TODO Because of this condition we currently only support SUM aggregations
        if (Sum *sum_agregation = dynamic_cast<Sum *>(window_definition_ptr->windowAggregation.get())) {
          if (partial_final_aggregates.size() <= w_id) {
            // initial the partial aggregate
            partial_final_aggregates[w_id] = partial_aggregates[s_id];
          } else {
            // update the partial aggregate
            partial_final_aggregates[w_id] =
                sum_agregation->combine<PartialAggregateType>(partial_final_aggregates[w_id], partial_aggregates[s_id]);
          }
        }
      }
    }
  }
  // calculate the final aggregate
  auto intBuffer = static_cast<FinalAggregateType *> (tuple_buffer->getBuffer());
  for (uint64_t i = 0; i < partial_final_aggregates.size(); i++) {
    //TODO Because of this condition we currently only support SUM aggregations
    if (Sum *sum_aggregation = dynamic_cast<Sum *>(window_definition_ptr->windowAggregation.get())) {
      intBuffer[i] = sum_aggregation->lower<FinalAggregateType, PartialAggregateType>(partial_final_aggregates[i]);
    }
  }
  tuple_buffer->setNumberOfTuples(partial_final_aggregates.size());
  store->setLastWatermark(watermark);
};

void WindowHandler::trigger() {
  auto window_state_variable = static_cast<StateVariable<int64_t, WindowSliceStore<int64_t> *> *>(this->window_state);
  // create the output tuple buffer
  auto tuple_buffer = BufferManager::instance().getBuffer();
  // iterate over all keys in the window state
  for (auto &it : window_state_variable->rangeAll()) {
    // write all window aggregates to the tuple buffer
    // TODO we currently have no handling in the case the tuple buffer is full
    this->aggregateWindows<int64_t, int64_t>(it.second, this->window_definition_ptr, tuple_buffer);
  }
  // send the tuple buffer to the next pipeline stage or sink
  Dispatcher::instance().addWork(tuple_buffer, this);
}

void *WindowHandler::getWindowState() {
  return window_state;
}

bool WindowHandler::start() {
  if (running)
    return false;
  running = true;

  IOTDB_DEBUG("WindowHandler " << this << ": Spawn thread")
  thread = std::thread(std::bind(&WindowHandler::trigger, this));
  return true;
}

bool WindowHandler::stop() {
  IOTDB_DEBUG("WindowHandler " << this << ": Stop called")

  if (!running)
    return false;
  running = false;

  if (thread.joinable())
    thread.join();
  IOTDB_DEBUG("WindowHandler " << this << ": Thread joinded")
  return true;
}


WindowHandler::~WindowHandler() {IOTDB_DEBUG("WINDOW: calling destructor")};

} // namespace NES
