#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <Runtime/YSBWindow.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::YSBWindow)
namespace iotdb{
YSBWindow::~YSBWindow()
{
	IOTDB_DEBUG("YSB Window: calling destructor")
}

YSBWindow::YSBWindow(size_t pCampaingCnt): windowSizeInSec(1), campaingCnt(pCampaingCnt)
{

};

YSBWindow::YSBWindow(): windowSizeInSec(1), campaingCnt(1)
{

}
}//end of namespace
