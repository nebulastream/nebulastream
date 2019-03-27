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

YSBWindow::YSBWindow(size_t pCampaingCnt, size_t windowSizeInSec): windowSizeInSec(windowSizeInSec),
        campaingCnt(pCampaingCnt), mutex(), currentWindow(0), lastChangeTimeStamp(time(NULL))
{
    IOTDB_DEBUG("YSB Window: calling destructor")
    setup();
};

YSBWindow::YSBWindow(): windowSizeInSec(1), campaingCnt(1), mutex(), currentWindow(0),lastChangeTimeStamp(time(NULL))
{
    IOTDB_DEBUG("YSB Window: calling default destructor")
    setup();
}



size_t YSBWindow::checkWindow(size_t actualWindow, size_t currentTime)
    {
        size_t nextWindow = currentWindow == 1 ? 0 : 1;
        std::unique_lock<std::mutex> lock(mutex);
        if(actualWindow == nextWindow)
        {
//            std::cout << "window already changed" << std::endl;
            //another thread did the change already
            return nextWindow;
        }
        else // change window
        {
//            std::fill(hashTable[currentWindow], hashTable[currentWindow]+campaingCnt, 0);
            if(currentTime != lastChangeTimeStamp)
            {
                currentWindow = nextWindow;
                lastChangeTimeStamp = time(NULL);
                print();
                return nextWindow;
            }
            else
            {
//                std::cout << "same window " << currentTime << std::endl;
                return currentWindow;
            }
        }
    }

}//end of namespace
