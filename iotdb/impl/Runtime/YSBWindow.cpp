#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <Runtime/YSBWindow.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::YSBWindow)

namespace iotdb{


	YSBWindow::YSBWindow(): windowSizeInSec(1), campaignCnt(1), currentWindow(0), lastTimestamp(time(nullptr))
	{

	}

	YSBWindow::YSBWindow(size_t pCampaignCnt): windowSizeInSec(1), campaignCnt(pCampaignCnt), currentWindow(0), lastTimestamp(time(nullptr))
	{

	}


	YSBWindow::YSBWindow(size_t pWindowSizeInSec, size_t pCampaignCnt): windowSizeInSec(pWindowSizeInSec), campaignCnt(pCampaignCnt), currentWindow(0), lastTimestamp(time(nullptr))
	{

	}

YSBWindow::~YSBWindow()
{
	IOTDB_DEBUG("YSB Window: calling destructor")
}

}//end of namespace
