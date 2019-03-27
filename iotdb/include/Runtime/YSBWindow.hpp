#ifndef INCLUDE_RUNTIME_WINDOWYSB_HPP_
#define INCLUDE_RUNTIME_WINDOWYSB_HPP_
#include <Runtime/Window.hpp>
#include <Util/Logger.hpp>
#include <atomic>
#include <iostream>
#include <Runtime/Window.hpp>
#include <mutex>
#include <condition_variable>

namespace iotdb{

class YSBWindow : public Window
{
public:
	YSBWindow();
	YSBWindow(size_t pCampaingCnt, size_t windowSizeInSec);

    ~YSBWindow();
    void setup()
    {
        hashTable = new std::atomic<size_t>*[2];
        hashTable[0] = new std::atomic<size_t>[campaingCnt + 1];
        for (size_t i = 0; i < campaingCnt + 1; i++)
            std::atomic_init(&hashTable[0][i], std::size_t(0));

		hashTable[1] = new std::atomic<size_t>[campaingCnt+1];
		for(size_t i = 0; i < campaingCnt+1; i++)
			  std::atomic_init(&hashTable[1][i],std::size_t(0));
	}
	void print()
	{
		IOTDB_INFO("windowSizeInSec=" << windowSizeInSec
		        << " campaingCnt=" << campaingCnt
		        << " currentWindow=" << currentWindow
		        << " lastChangeTimeStamp=" << lastChangeTimeStamp)
		IOTDB_INFO("Hash Table Content with window 1:")
		for(size_t i = 0; i < campaingCnt; i++)
		{
			if(hashTable[0][i] != 0)
				IOTDB_INFO("id=" << i << " cnt=" << hashTable[0][i])
		}

        IOTDB_INFO("Hash Table Content with window 2:")
        for (size_t i = 0; i < campaingCnt; i++) {
            if (hashTable[1][i] != 0)
                IOTDB_INFO("id=" << i << " cnt=" << hashTable[1][i])
        }
    }

    size_t getNumberOfEntries()
    {
        size_t numEntries = 0;
        for (size_t i = 0; i < campaingCnt; i++) {
            if (hashTable[0][i] != 0)
                numEntries += hashTable[0][i];
        }

        IOTDB_INFO("Hash Table Content with window 2:")
        for (size_t i = 0; i < campaingCnt; i++) {
            if (hashTable[1][i] != 0)
                numEntries += hashTable[1][i];
        }
        return numEntries;
    }

	void shutdown()
	{
		delete[] hashTable[0];
		delete[] hashTable[1];
	}

 	template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{
        ar & boost::serialization::base_object<Window>(*this);
		ar & windowSizeInSec;
		ar & campaingCnt;

	}

	std::atomic<size_t>** getHashTable(){return hashTable;};
	size_t getWindowSizeInSec(){return windowSizeInSec;};
    size_t getCampaingCnt(){return campaingCnt;};

    size_t checkWindow(size_t actualWindow, size_t currentTime);

    size_t getCurrentWindow()
    {
        return currentWindow;
    }

    size_t getLastChangeTimeStamp()
    {
        return lastChangeTimeStamp;
    }
private:
    friend class boost::serialization::access;
    std::mutex mutex;
    std::atomic<size_t> currentWindow;
    std::atomic<size_t> lastChangeTimeStamp;

    std::atomic<size_t>** hashTable;
    size_t windowSizeInSec;
    size_t campaingCnt;


};
} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::YSBWindow)
#endif /* INCLUDE_RUNTIME_WINDOW_HPP_ */
