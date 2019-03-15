#include <Runtime/Window.hpp>

namespace iotdb{
const WindowPtr createTestWindow(size_t campainCnt) {

	  WindowPtr win(new YSBWindow(campainCnt));

	  return win;
	}

 Window::~Window(){
	 IOTDB_DEBUG("WINDOW: calling destructor")
 };



YSBWindow::~YSBWindow()
 {
	 IOTDB_DEBUG("YSB Window: calling destructor")

 }

}
