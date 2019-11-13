#include "../../include/Windows/Window.hpp"

#include "../../include/YSB_legacy/YSBWindow.hpp"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
#include <State/StateManager.hpp>
#include <NodeEngine/BufferManager.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::Window)
namespace iotdb{

void Window::setup() {
  // Initialize Window Manager
  this->window_manager_ptr_ = std::make_shared<WindowManager>(this->window_definition_ptr);
  // Initialize StateVariable
  auto& state_manager = StateManager::instance();
  this->window_state = (void *) &state_manager.registerState<int64_t, WindowSliceStore<int64_t> *>("window");
}

template<class FinalAggregateType, class PartialAggregateType>
std::vector<FinalAggregateType> aggregateWindows(uint64_t watermark,
                                                 WindowSliceStore<PartialAggregateType> *store, WindowDefinitionPtr window_definition_ptr){

  auto windows = std::make_shared<std::vector<WindowState>>();
  window_definition_ptr->windowType->triggerWindows(windows, 0, watermark);
  auto finalAggregates = std::vector<FinalAggregateType>();
  auto slices = store->getSliceMetadata();
  auto partialAggregates = store->getPartialAggregates();
  for (int s_id = 0; s_id < slices.size(); s_id++) {
    for (int w_id = 0; w_id < windows->size(); w_id++) {
      auto window = (*windows)[w_id];
      // A slice is contained in a window if the window starts before the slice and ends after the slice
      if (window.getStartTs() <= slices[s_id].getStartTs() &&
          window.getEndTs() >= slices[s_id].getEndTs()) {

        if (Sum *sumAggregation = dynamic_cast<Sum *>(window_definition_ptr->windowAggregation.get())) {
          auto partialAggregate = partialAggregates[s_id];
          finalAggregates[s_id] =
              sumAggregation->combine<PartialAggregateType>(finalAggregates[s_id], partialAggregate);
        }
        //auto combined = store.combine<PartialAggregateType>(partial, partial2);
      }
    }
  }
  return finalAggregates;
};


void Window::trigger()
{
  auto window_state_variable = static_cast<StateVariable<int64_t, WindowSliceStore<int64_t>*>*>(this->window_state);
  auto tuple_buffer = BufferManager::instance().getBuffer();
  for (auto& it : window_state_variable->rangeAll()) {
    auto windowAggregates = aggregateWindows<int64_t, int64_t>(0, it.second, this->window_definition_ptr);
    for(auto& aggregate : windowAggregates){
      // TODO put window aggregates in tuple buffer.
    }
  }
  // TODO send buffer to next pipeline
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


const WindowPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec) {

    WindowPtr win(new YSBWindow(campainCnt, windowSizeInSec));

    return win;
}

Window::~Window(){IOTDB_DEBUG("WINDOW: calling destructor")};

} // namespace iotdb
