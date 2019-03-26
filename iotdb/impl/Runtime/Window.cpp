#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <Runtime/YSBWindow.hpp>
#include <Runtime/Window.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::Window)
namespace iotdb{


const WindowPtr createTestWindow(size_t campainCnt) {

	  WindowPtr win(new YSBWindow(campainCnt));

	  return win;
	}

 Window::~Window(){
	 IOTDB_DEBUG("WINDOW: calling destructor")
 };


}
