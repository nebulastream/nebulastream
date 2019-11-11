#include "../../include/Windows/Window.hpp"

#include "../../include/YSB_legacy/YSBWindow.hpp"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::Window)
namespace iotdb{

void Window::trigger()
{
  //do something like wake up every second and check if we have to do something
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
