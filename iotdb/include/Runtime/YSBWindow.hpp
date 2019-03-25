#ifndef INCLUDE_RUNTIME_WINDOWYSB_HPP_
#define INCLUDE_RUNTIME_WINDOWYSB_HPP_

#include <memory>
#include <atomic>
#include <iostream>

#include <Util/Logger.hpp>
#include <Runtime/Window.hpp>

namespace iotdb{

class YSBWindow : public Window
{
public:

	YSBWindow();
	YSBWindow(size_t pCampaignCnt);
	YSBWindow(size_t pWindowSizeInSec, size_t pCampaignCnt);
	~YSBWindow();

	void setup()
	{
		hashTable = new std::atomic<size_t>*[2];
		hashTable[0] = new std::atomic<size_t>[campaignCnt+1];
		for(size_t i = 0; i < campaignCnt+1; i++)
			  std::atomic_init(&hashTable[0][i],std::size_t(0));

		hashTable[1] = new std::atomic<size_t>[campaignCnt+1];
		for(size_t i = 0; i < campaignCnt+1; i++)
			  std::atomic_init(&hashTable[1][i],std::size_t(0));
	}

	void print()
	{
		IOTDB_INFO("windowSizeInSec=" << windowSizeInSec << " campaignCnt=" << campaignCnt)
		IOTDB_INFO("Hash Table Content with window 1:")
		for(size_t i = 0; i < campaignCnt; i++)
		{
			if(hashTable[0][i] != 0)
				IOTDB_INFO("id=" << i << " cnt=" << hashTable[0][i])
		}

		IOTDB_INFO("Hash Table Content with window 2:")
		for(size_t i = 0; i < campaignCnt; i++)
		{
			if(hashTable[1][i] != 0)
				IOTDB_INFO("id=" << i << " cnt=" << hashTable[1][i])
		}
	}

	size_t getNumberOfEntries()
	{
		size_t numEntries = 0;
		for(size_t i = 0; i < campaignCnt; i++)
		{
			if(hashTable[0][i] != 0)
				numEntries += hashTable[0][i];
		}

		IOTDB_INFO("Hash Table Content with window 2:")
		for(size_t i = 0; i < campaignCnt; i++)
		{
			if(hashTable[1][i] != 0)
				numEntries += hashTable[1][i];
		}
		return numEntries;
	}

	const size_t getWindowSizeSec() const { return windowSizeInSec; }

	const size_t getCampaignCount() const { return campaignCnt; }

	const size_t getCurrentWindow() const { return currentWindow; }

	void setCurrentWindow(size_t pCurrentWindow) { currentWindow = pCurrentWindow; }

	const size_t getLastTimestamp() const { return lastTimestamp; }

	void setLastTimestamp(size_t pLastTimestamp) { lastTimestamp = pLastTimestamp; }

	void shutdown()
	{
//		IOTDB_INFO("Final Window Result:");
//		print();
		//maybe also delete the entries?
		delete[] hashTable[0];
		delete[] hashTable[1];
	}

 	template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{
        ar & boost::serialization::base_object<Window>(*this);
		ar & windowSizeInSec;
		ar & campaignCnt;
	}

	std::atomic<size_t>** getHashTable(){return hashTable;};

private:
    friend class boost::serialization::access;

    std::atomic<size_t>** hashTable;

	size_t windowSizeInSec;
	size_t campaignCnt;
	size_t currentWindow;
	size_t lastTimestamp;
};
}//end of namespace

#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::YSBWindow)

#endif /* INCLUDE_RUNTIME_WINDOW_HPP_ */
