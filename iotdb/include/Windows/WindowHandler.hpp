#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_


#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <API/Window/WindowDefinition.hpp>


#include <thread>
#include <QueryLib/WindowManagerLib.hpp>
#include <NodeEngine/BufferManager.hpp>

namespace iotdb {

/**
 * @brief This represents a window during query execution.
 */
class WindowHandler {
 public:
  WindowHandler() = default;
  WindowHandler(WindowDefinitionPtr window_definition_ptr);
  ~WindowHandler();

  /**
   * @brief Initialises the state of this window depending on the window definition.
   */
  void setup();

  /**
   * @brief Starts thread to check if the window should be triggered.
   * @return boolean if the window thread is started
   */
  bool start();

  /**
   * @brief Stops the window thread.
   * @return
   */
  bool stop();

  /**
   * @brief triggers all ready windows.
   * @return
   */
  void trigger();

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
  void aggregateWindows(WindowSliceStore<PartialAggregateType> *store,
                        WindowDefinitionPtr window_definition_ptr,
                        TupleBufferPtr tuple_buffer);

  void *getWindowState();
  WindowManagerPtr getWindowManager() {
    return window_manager_ptr;
  };

  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {}

 private:
  friend class boost::serialization::access;
  bool running;
  WindowDefinitionPtr window_definition_ptr;
  WindowManagerPtr window_manager_ptr;
  void *window_state;
  std::thread thread;

};

typedef std::shared_ptr<WindowHandler> WindowPtr;

//just for test compability
const WindowPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec);

} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::WindowHandler)
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
