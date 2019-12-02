#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <atomic>
#include <iostream>
#include <memory>

#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <API/Window/WindowDefinition.hpp>

#include <Util/Logger.hpp>
#include <thread>
#include <QueryLib/WindowManagerLib.hpp>
#include <NodeEngine/BufferManager.hpp>

namespace iotdb {

/**
 * This represents a window during query execution.
 */
class Window {
 public:
  Window() = default;
  Window(WindowDefinitionPtr window_definition_ptr);
  ~Window();

  /**
   * Initialises the state of this window depending on the window definition.
   */
  void setup();

  /**
   * Starts thread to check if the window should be triggered.
   * @return boolean if the window thread is started
   */
  bool start();

  /**
   * Stops the window thread.
   * @return
   */
  bool stop();

  /**
   * @brief triggers all ready windows.
   * @return
   */
  void trigger();

  /**
   * Processes window aggregates and write results to tuple buffer.
   * @tparam FinalAggregateType
   * @tparam PartialAggregateType
   * @param store
   * @param window_definition_ptr
   * @param tuple_buffer
   */
  template<class FinalAggregateType, class PartialAggregateType>
  void aggregateWindows(WindowSliceStore <PartialAggregateType> *store,
                        WindowDefinitionPtr window_definition_ptr,
                        TupleBufferPtr tuple_buffer);

  void *getWindowState();
  WindowManagerPtr getWindowManager() {
    return window_manager_ptr_;
  };

  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {}

 private:
  friend class boost::serialization::access;
  bool running;
  WindowDefinitionPtr window_definition_ptr;
  WindowManagerPtr window_manager_ptr_;
  void *window_state;
  std::thread thread;

};

typedef std::shared_ptr<Window> WindowPtr;

//just for test compability
const WindowPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec);

} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::Window)
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
