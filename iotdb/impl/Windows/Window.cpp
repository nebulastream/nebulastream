#include "../../include/Windows/Window.hpp"

#include "../../include/YSB_legacy/YSBWindow.hpp"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::Window)
namespace iotdb{


const WindowPtr createTestWindow(size_t campainCnt, size_t windowSizeInSec) {

    WindowPtr win(new YSBWindow(campainCnt, windowSizeInSec));

    return win;
}

Window::~Window(){IOTDB_DEBUG("WINDOW: calling destructor")};

} // namespace iotdb
