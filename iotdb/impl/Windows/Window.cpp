#include "../../include/Windows/Window.hpp"

#include "../../include/YSB_legacy/YSBWindow.hpp"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
#include <State/StateManager.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Dispatcher.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::Window)
namespace iotdb {

void Window::setup() {
  // Initialize Window Manager
  this->window_manager_ptr_ = std::make_shared<WindowManager>(this->window_definition_ptr);
  // Initialize StateVariable
  auto &state_manager = StateManager::instance();
  this->window_state = &state_manager.registerState<int64_t, WindowSliceStore<int64_t> *>("window");
}

/**
 * @brief This method iterates over all slices in the slice store and creates the final window aggregates,
 * which are written to the tuple buffer.
 * @tparam FinalAggregateType
 * @tparam PartialAggregateType
 * @param watermark
 * @param store
 * @param window_definition_ptr
 * @param tuple_buffer
 */
template<class FinalAggregateType, class PartialAggregateType>
void aggregateWindows(uint64_t watermark,
                      WindowSliceStore<PartialAggregateType> *store,
                      WindowDefinitionPtr window_definition_ptr,
                      TupleBufferPtr tuple_buffer) {

  auto intBuffer = static_cast<FinalAggregateType *> (tuple_buffer->getBuffer());
  auto windows = std::make_shared<std::vector<WindowState>>();
  window_definition_ptr->windowType->triggerWindows(windows, store->getLastWatermark(), watermark);
  auto partialFinalAggregates = std::vector<PartialAggregateType>();
  auto slices = store->getSliceMetadata();
  auto partialAggregates = store->getPartialAggregates();
  for (uint64_t s_id = 0; s_id < slices.size(); s_id++) {
    for (uint64_t w_id = 0; w_id < windows->size(); w_id++) {
      auto window = (*windows)[w_id];
      // A slice is contained in a window if the window starts before the slice and ends after the slice
      if (window.getStartTs() <= slices[s_id].getStartTs() &&
          window.getEndTs() >= slices[s_id].getEndTs()) {

        //TODO Because of this condition we currently only support SUM aggregations
        if (Sum *sumAggregation = dynamic_cast<Sum *>(window_definition_ptr->windowAggregation.get())) {
          auto partialAggregate = partialAggregates[s_id];
          partialFinalAggregates[s_id] =
              sumAggregation->combine<PartialAggregateType>(partialFinalAggregates[s_id], partialAggregate);
        }
      }
    }
  }
  for (uint64_t i = 0; i < partialFinalAggregates.size(); i++) {
    //TODO Because of this condition we currently only support SUM aggregations
    if (Sum *sumAggregation = dynamic_cast<Sum *>(window_definition_ptr->windowAggregation.get())) {
      intBuffer[i] = sumAggregation->lower<FinalAggregateType, PartialAggregateType>(partialFinalAggregates[i]);
    }
  }
  store->setLastWatermark(watermark);
};

void Window::trigger() {
  auto window_state_variable = static_cast<StateVariable<int64_t, WindowSliceStore<int64_t> *> *>(this->window_state);
  // create the output tuple buffer
  auto tuple_buffer = BufferManager::instance().getBuffer();
  // iterate over all keys in the window state
  for (auto &it : window_state_variable->rangeAll()) {
    // write all window aggregates to the tuple buffer
    // TODO we currently have no handling in the case the tuple buffer is full
    aggregateWindows<int64_t, int64_t>(0, it.second, this->window_definition_ptr, tuple_buffer);
  }
  // send the tuple buffer to the next pipeline stage or sink
  Dispatcher::instance().addWork(tuple_buffer, this);
}

void *Window::getWindowState() {
  return window_state;
}

bool Window::start() {
  if (running)
    return false;
  running = true;

  IOTDB_DEBUG("Window " << this << ": Spawn thread")
  thread = std::thread(std::bind(&Window::trigger, this));
  return true;
}

bool Window::stop() {
  IOTDB_DEBUG("Window " << this << ": Stop called")

  if (!running)
    return false;
  running = false;

  if (thread.joinable())
    thread.join();
  IOTDB_DEBUG("Window " << this << ": Thread joinded")
  return true;
}

void Window::print() {}

 size_t Window::getNumberOfEntries() {
   return 0;
}

const WindowPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec) {

  WindowPtr win(new YSBWindow(campainCnt, windowSizeInSec));

  return win;
}

Window::~Window() {IOTDB_DEBUG("WINDOW: calling destructor")};

} // namespace iotdb
