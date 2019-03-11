#include <Runtime/Window.hpp>

namespace iotdb{
const WindowPtr createTestWindow() {

	  WindowPtr win(new YSBWindow());

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
