#include <API/Window/WindowTrigger.hpp>
#include <API/AbstractWindowDefinition.hpp>
#include <State/StateManager.hpp>
#include <NodeEngine/BufferManager.hpp>

namespace iotdb {

void WindowTrigger::setup() {
  window_manager_ptr_ = std::make_shared<WindowManager>(window_definition_ptr, 0);
}

void WindowTrigger::trigger() {
  auto window_state = static_cast<StateVariable<int64_t, WindowSliceStore<int64_t>*>*>(this->window_state);

  auto window_slice_store =
      static_cast<WindowSliceStore<int64_t> *>(this->window_state);

  auto windowAggregates = aggregateWindows<int64_t, int64_t>(0, window_slice_store);

  auto tupleBuffer = BufferManager::instance().getBuffer();

}

}
