#ifndef INCLUDE_RUNTIME_WINDOW_HPP_
#define INCLUDE_RUNTIME_WINDOW_HPP_
#include <memory>
#include <Util/Logger.hpp>
#include <atomic>
#include <iostream>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/shared_ptr.hpp>


namespace iotdb{
class Window;
typedef std::shared_ptr<Window> WindowPtr;

class Window {
public:
	virtual ~Window();
	virtual void setup() = 0;
	virtual void print() = 0;
	virtual void shutdown() = 0;
	virtual size_t getNumberOfEntries() = 0;

	template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{
	}


private:
};
const WindowPtr createTestWindow(size_t pCampaingCnt);



class YSBWindow : public Window
{
public:

	YSBWindow(size_t pCampaingCnt): windowSizeInSec(1), campaingCnt(pCampaingCnt)
	{
	};

	~YSBWindow();
	void setup()
	{
		hashTable = new std::atomic<size_t>*[2];
		hashTable[0] = new std::atomic<size_t>[campaingCnt+1];
		for(size_t i = 0; i < campaingCnt+1; i++)
			  std::atomic_init(&hashTable[0][i],std::size_t(0));

		hashTable[1] = new std::atomic<size_t>[campaingCnt+1];
		for(size_t i = 0; i < campaingCnt+1; i++)
			  std::atomic_init(&hashTable[1][i],std::size_t(0));
	}
	void print()
	{
		IOTDB_INFO("Hash Table Content with window 1:")
		for(size_t i = 0; i < campaingCnt; i++)
		{
			if(hashTable[0][i] != 0)
				IOTDB_INFO("id=" << i << " cnt=" << hashTable[0][i])
		}

		IOTDB_INFO("Hash Table Content with window 2:")
		for(size_t i = 0; i < campaingCnt; i++)
		{
			if(hashTable[1][i] != 0)
				IOTDB_INFO("id=" << i << " cnt=" << hashTable[1][i])
		}
	}

	size_t getNumberOfEntries()
	{
		size_t numEntries = 0;
		for(size_t i = 0; i < campaingCnt; i++)
		{
			if(hashTable[0][i] != 0)
				numEntries += hashTable[0][i];
		}

		IOTDB_INFO("Hash Table Content with window 2:")
		for(size_t i = 0; i < campaingCnt; i++)
		{
			if(hashTable[1][i] != 0)
				numEntries += hashTable[1][i];
		}
		return numEntries;
	}

	void shutdown()
	{
//		IOTDB_INFO("Final Window Result:");
//		print();
		//maybe also delete the entries?
		delete[] hashTable[0];
		delete[] hashTable[1];
	}

	std::atomic<size_t>** getHashTable(){return hashTable;};

private:
    std::atomic<size_t>** hashTable;
    size_t windowSizeInSec;
    size_t campaingCnt;

};

}//end of namespace
#endif /* INCLUDE_RUNTIME_WINDOW_HPP_ */
