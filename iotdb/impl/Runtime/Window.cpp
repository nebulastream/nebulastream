#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>

#include <Runtime/Window.hpp>
#include <Runtime/YSBWindow.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::Window)

namespace iotdb {

const WindowPtr createTestWindow(size_t pCampaignCnt)
{
    WindowPtr win(new YSBWindow(pCampaignCnt));
    return win;
}

const WindowPtr createTestWindow(size_t pWindowSizeInSec, size_t pCampaignCnt)
{
    WindowPtr win(new YSBWindow(pWindowSizeInSec, pCampaignCnt));
    return win;
}

Window::~Window(){IOTDB_DEBUG("WINDOW: calling destructor")};

} // namespace iotdb