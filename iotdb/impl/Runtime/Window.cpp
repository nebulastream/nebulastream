#include <Runtime/Window.hpp>

namespace iotdb {
const WindowPtr createTestWindow(size_t pWindowSizeInSec, size_t pCampaignCnt) {

  WindowPtr win(new YSBWindow(pWindowSizeInSec, pCampaignCnt));

  return win;
}

Window::~Window(){IOTDB_DEBUG("WINDOW: calling destructor")};

YSBWindow::~YSBWindow() { IOTDB_DEBUG("YSB Window: calling destructor") }

} // namespace iotdb
