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

namespace iotdb {
class Window;
typedef std::shared_ptr<Window> WindowPtr;

class Window {
 public:
  ~Window();
  Window()= default;
  Window(WindowDefinitionPtr window_definition_ptr);
  void setup();
  bool start();
  bool stop();
  void trigger();

  void print();

  size_t getNumberOfEntries();

  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {}

  void * getWindowState();
  WindowManagerPtr getWindowManager(){
    return window_manager_ptr_;
  };

 private:
  friend class boost::serialization::access;
  bool running;
  WindowDefinitionPtr window_definition_ptr;
  WindowManagerPtr window_manager_ptr_;
  void *window_state;
  std::thread thread;
};

//just for test compability
const WindowPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec);

} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::Window)
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
