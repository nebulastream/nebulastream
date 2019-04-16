#include <iostream>
#include <cstring>
#include <omp.h>
#include <chrono>
#include <random>
#include <bitset>
#include <algorithm>
#include <atomic>
#include <array>
#include <assert.h>
#include <sstream>
#include <fstream>
#include <tbb/concurrent_queue.h>
#include <thread>

//#define BUFFER_SIZE 1000
std::atomic<size_t> exitProgram;

using namespace std;
typedef uint64_t Timestamp;
using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;

struct __attribute__((packed)) record {
    uint8_t user_id[16];
    uint8_t page_id[16];
    uint8_t campaign_id[16];
    char event_type[9];
    char ad_type[9];
    int64_t current_ms;
    uint32_t ip;

    record(){
    	event_type[0] = '-';//invalid record
    	current_ms = 0;
    	ip = 0;
    }

    record(const record& rhs)
    {
    	memcpy(&user_id, &rhs.user_id, 16);
    	memcpy(&page_id, &rhs.page_id, 16);
    	memcpy(&campaign_id, &rhs.campaign_id, 16);
    	memcpy(&event_type, &rhs.event_type, 9);
    	memcpy(&ad_type, &rhs.ad_type, 9);
    	current_ms = rhs.current_ms;
    	ip = rhs.current_ms;
    }

};//size 78 bytes

union tempHash
{
	uint64_t value;
	char buffer[8];
};

struct Tuple
{
	Tuple()
	{
		//campaign_id[0] = '-';//empty campain
		campaign_id = 0;
		timeStamp = std::numeric_limits<std::size_t>::max();
	}
	Tuple(uint64_t pCampaign_id, size_t pTimeStamp)
	{
		campaign_id = pCampaign_id;
		//memcpy(&campaign_id, pCampaign_id, sizeof(long));
		timeStamp = pTimeStamp;
	}

	//size_t campaign_id;
	uint64_t campaign_id;
	size_t timeStamp;
};//size 16 Byte


struct __attribute__((packed)) Buffer
{
    Buffer()
    {
        bufferSize = 0;
        pos = 0;
        content = nullptr;
    }

    Buffer(const Buffer& other)
    {
//        cout << "call copy" << endl;
        bufferSize = other.bufferSize;
        pos = other.bufferSize;
        if(content == nullptr)
        {
            content = new Tuple[bufferSize];
        }
        memcpy(content, other.content, other.bufferSize * sizeof(Tuple));

    } // user-defined copy ctor


	Buffer(size_t pBufferSize)
	{
	    bufferSize = pBufferSize;
		pos = 0;
		content = new Tuple[bufferSize];
	}

	bool add(Tuple& tup)
	{
		content[pos++] = tup;
		return pos == bufferSize;
	}

  size_t bufferSize;
  size_t pos;
  Tuple* content;

};


void shuffle(record* array, size_t n)
{
    if (n > 1)
    {
        size_t i;
        for (i = 0; i < n - 1; i++)
        {
          size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
          record t = array[j];
          array[j] = array[i];
          array[i] = t;
        }
    }
}

static const std::string events[] = {"view", "click", "purchase"};
void generate(record& data, size_t campaingOffset, uint64_t campaign_lsb, uint64_t campaign_msb, size_t event_id)
{
  event_id = event_id % 3;

  memcpy(data.campaign_id, &campaign_msb, 8);

  uint64_t campaign_lsbr = campaign_lsb + campaingOffset;
  memcpy(&data.campaign_id[8], &campaign_lsbr, 8);

  const char* str = events[event_id].c_str();
  strcpy(&data.ad_type[0], "banner78");
  strcpy(&data.event_type[0], str);

  auto ts = std::chrono::system_clock::now().time_since_epoch();
  data.current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(ts).count();

  data.ip = 0x01020304;
}

Timestamp getTimestamp()
{
  return std::chrono::duration_cast<NanoSeconds>(
			 Clock::now().time_since_epoch())
	  .count();
}

void produce_window_mem(size_t processCnt, record* records, tbb::concurrent_bounded_queue<Buffer>** queue,
        size_t consumeCnt, size_t prodID, size_t bufferSize)
{
	size_t produced = 0;
	size_t disQTuple = 0;
	size_t qualTuple = 0;
	size_t pushCnt = 0;

	size_t sender[consumeCnt] = {0};
	Buffer** tempBuffers = new Buffer*[consumeCnt];
	for(size_t i = 0; i < consumeCnt; i++)
	{
	    tempBuffers[i] = new Buffer(bufferSize);
	}
//	std::vector<Buffer> tempBuffers(consumeCnt, Buffer(bufferSize));
//	Buffer tempBuffers[consumeCnt] = {bufferSize};

	for(size_t i = 0; i < processCnt; i++)
	{
			uint32_t value = *((uint32_t*) records[i].event_type);
			if(value != 2003134838)
			{
				produced++;
				disQTuple++;
				continue;
			}

			qualTuple++;
			produced++;
			size_t timeStamp = time(NULL);
			tempHash hashValue;
			hashValue.value = *(((uint64_t*) records[i].campaign_id) + 1);
			Tuple tup(hashValue.value, timeStamp);
//			cout << "hash value= " << hashValue.value  << " bucket=" <<hashValue.value % consumeCnt << endl;
			if(tempBuffers[hashValue.value % consumeCnt]->add(tup))
            {
                pushCnt++;
                queue[hashValue.value % consumeCnt]->push(*
                        tempBuffers[hashValue.value % consumeCnt]);
                tempBuffers[hashValue.value % consumeCnt]->pos = 0;
                sender[hashValue.value % consumeCnt]++;
            }
	}

	//send unfull buffer
    for(size_t i = 0; i < consumeCnt; i++)
    {
        if(tempBuffers[i]->pos != 0)
        {
//            cout << "send remaining buffer " << i  << " with " << tempBuffers[i]->pos << " tuples left."<< endl;
            pushCnt++;
            tempHash hashValue;
            hashValue.value = tempBuffers[i]->content[0].campaign_id;
//            cout << "sending to queue=" << hashValue.value % consumeCnt << endl;
            queue[hashValue.value % consumeCnt]->push(*tempBuffers[i]);
            tempBuffers[hashValue.value % consumeCnt]->pos = 0;
            sender[hashValue.value % consumeCnt]++;
        }
    }
	stringstream ss;
	ss << "Thread=" << omp_get_thread_num() << " prodID=" << prodID <<" produced=" << produced << " pushCnt=" << pushCnt
			<< " disQTuple=" << disQTuple << " qualTuple=" << qualTuple;

	for(size_t i = 0; i < consumeCnt; i++)
	{
		ss << " send_" << i << "=" << sender[i];
	}
	cout << ss.str() << endl;

	std::atomic_fetch_add(&exitProgram, size_t(1));
}

void cosume_window_mem(std::atomic<size_t>** hashTable,
		size_t windowSizeInSec, tbb::concurrent_bounded_queue<Buffer>** queue, size_t campaingCnt, size_t* consumedRet,
		size_t consumerID, size_t produceCnt, size_t bufferSize)
{
	size_t consumed = 0;
	size_t windowSwitchCnt = 0;
	size_t htReset = 0;
	size_t lastTimeStamp = 0;
	size_t popCnt = 0;
	Tuple tup;
	Buffer buff(bufferSize);
	bool consume = true;

	while(consume)
	{
        size_t current_window = 0;

		while(!queue[consumerID]->empty())
		{
			queue[consumerID]->pop(buff);
//			cout << "Consumer" << consumerID << " consume from to queue " << consumerID << endl;

			popCnt++;
			size_t timeStamp = time(NULL);//seconds elapsed since 00:00 hours, Jan 1, 1970 UTC

			if(lastTimeStamp != timeStamp && timeStamp % windowSizeInSec == 0)
			{
				//increment to new window
			    current_window = current_window == 0 ? 1 : 0;

			    windowSwitchCnt++;
				cout << "thread=" << omp_get_thread_num() << " in first loop" << endl;
				if(hashTable[current_window][campaingCnt] != timeStamp)
				{
				    //TODO: potentiall race here
					htReset++;
					cout << "thread=" << omp_get_thread_num() << " in second loop" << endl;
					atomic_store(&hashTable[current_window][campaingCnt], timeStamp);
					std::fill(hashTable[current_window], hashTable[current_window]+campaingCnt, 0);
					//memset(myarray, 0, N*sizeof(*myarray)); // TODO: is it faster?
				}
				lastTimeStamp = timeStamp;
			}
			for(size_t u = 0; u < bufferSize; u++)
			{
				//consume one tuple
				uint64_t bucketPos = (buff.content[u].campaign_id * 789 + 321)% campaingCnt;
				atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
				consumed++;
			}
		}//end of while not empty

		if(std::atomic_load(&exitProgram) == produceCnt)
			consume = false;
	}

	stringstream ss;
	ss << "Thread=" << omp_get_thread_num() << " consumed=" << consumed
			<< " popCnt=" << popCnt
			<< " windowSwitchCnt=" << windowSwitchCnt
			<< " htreset=" << htReset;
	cout << ss.str() << endl;
	*consumedRet = consumed;
}



int main(int argc, char *argv[])
{
	cout << "processCnt bufferSize numProducer numberConsumer " << endl;
	//initialze
	const size_t campaingCnt = 10000;

	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<size_t> diss(0, SIZE_MAX);

	size_t processCnt = 0;
	size_t num_Consumer = 0;
	size_t num_Producer = 0;
	size_t bufferSize = 0;
	size_t windowSizeInSeconds = 1;

	if(argc != 1)
	{
		processCnt = atoi(argv[1]);
		bufferSize = atoi(argv[2]);
		num_Producer = atoi(argv[3]);
		num_Consumer = atoi(argv[4]);
	}

	size_t threadCnt = num_Consumer + num_Producer;

	size_t bufferCnt = processCnt/ bufferSize;
	cout << "param processCnt=" << processCnt << " producer" << num_Producer << " consumer=" << num_Consumer
	        << " bufferSize=" << bufferSize
			<< " bufferCnt=" << bufferCnt <<  endl;

//Generator Code
	size_t randomCnt = processCnt/10;
	size_t* randomNumbers = new size_t[randomCnt];
	std::uniform_int_distribution<size_t> disi(0, campaingCnt);
	for(size_t i = 0; i < randomCnt; i++)
		randomNumbers[i] = disi(gen);

	record** recs;
	uint64_t campaign_lsb, campaign_msb;
	auto uuid = diss(gen);
	uint8_t* uuid_ptr = reinterpret_cast<uint8_t*>(&uuid);
	memcpy(&campaign_msb, uuid_ptr, 8);
	memcpy(&campaign_lsb, uuid_ptr + 8, 8);
	campaign_lsb &= 0xffffffff00000000;

	recs = new record*[num_Producer];
	for(size_t i = 0; i < num_Producer; i++)
	{
		recs[i] = new record[processCnt];

		for(size_t u = 0; u < processCnt; u++)
		{
			generate(recs[i][u], /**campaingOffset*/ randomNumbers[u%randomCnt], campaign_lsb, campaign_msb, /**eventID*/ u);
		}
		shuffle(recs[i], processCnt);
	}

//create hash table
	std::atomic<size_t>** hashTable = new std::atomic<size_t>*[2];
	hashTable[0] = new std::atomic<size_t>[campaingCnt+1];
	for(size_t i = 0; i < campaingCnt+1; i++)
		  std::atomic_init(&hashTable[0][i],std::size_t(0));

	hashTable[1] = new std::atomic<size_t>[campaingCnt+1];
	for(size_t i = 0; i < campaingCnt+1; i++)
		  std::atomic_init(&hashTable[1][i],std::size_t(0));

	tbb::concurrent_bounded_queue<Buffer>** tbbQueues = new tbb::concurrent_bounded_queue<Buffer>*[num_Consumer];
	for(size_t i = 0; i < num_Consumer; i++)
	{
		tbbQueues[i] = new tbb::concurrent_bounded_queue<Buffer>();
	}

	size_t* consumed = new size_t[num_Consumer];

	Timestamp begin = getTimestamp();

#pragma omp parallel num_threads(threadCnt)
	{
		#pragma omp for
		for(size_t i = 0; i < threadCnt; i++)
		{
			if(i < num_Producer)
			{
                produce_window_mem(processCnt, recs[i], tbbQueues, num_Consumer, i, bufferSize);
			}
			else
			{
				cosume_window_mem(hashTable, windowSizeInSeconds, tbbQueues, campaingCnt, &consumed[i - num_Producer], i - num_Producer,
						num_Producer, bufferSize);
			}
		}
	}
	Timestamp end = getTimestamp();

	double elapsed_time = double(end - begin) / (1024 * 1024 * 1024);
	size_t consumedOverall = 0;
	for (size_t i = 0; i < num_Consumer ; i++)
	{
		cout << "con " << i << ":" << consumed[i] << endl;
		consumedOverall += consumed[i];
	}
	cout << " time=" <<  elapsed_time << " produced=" << num_Producer * processCnt
	 << " throughput=" << num_Producer * processCnt/ elapsed_time
	 << " consumedOverall=" << consumedOverall << " consumeRarte=" << consumedOverall/elapsed_time << endl;
}
