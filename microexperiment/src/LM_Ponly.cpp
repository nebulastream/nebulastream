#pragma GCC optimize("O3","unroll-loops","omit-frame-pointer","inline") //Optimization flags
#pragma GCC option("arch=native","tune=native","no-zero-upper") //Enable AVX
#pragma GCC target("avx", "avx512cd", "avx512cd" ,"avx512f")  //Enable AVX
#include <string>

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
#include "DeviceMemoryAllocator/CPUMemoryAllocator.h"
#include "MPIHelper.h"
#include <NumaUtilities.h>
#include <ConnectionInfoProvider/SimpleInfoProvider.h>
#include <VerbsConnection.h>
#include <TimeTools.hpp>
#include <memory>
#include "DataExchangeOperators/AbstractDataExchangeOperator.h"
#include <future>
#include <boost/program_options.hpp>
#include <mutex>
#include <numaif.h>
#include <atomic>

//#define BUFFER_SIZE 1000

using namespace std;

#define PORT1 55355
#define PORT2 55356
#define PORT3 55357

//#define OLCONSUMERVERSION
//#define BUFFER_COUNT 10
#define BUFFER_USED_SENDER_DONE 127
#define BUFFER_READY_FLAG 0
#define BUFFER_USED_FLAG 1
#define BUFFER_BEING_PROCESSED_FLAG 2
#define NUMBER_OF_GEN_TUPLE 1000000
//#define DEBUG
void printSingleHT(std::atomic<size_t>* hashTable, size_t campaingCnt);
std::vector<std::shared_ptr<std::thread>> buffer_threads;

//VerbsConnection* sharedHTConnection;
infinity::memory::RegionToken** sharedHT_region_token;
infinity::memory::Buffer** sharedHT_buffer;
std::atomic<size_t>* outputTable;


struct __attribute__((packed)) record {
    uint8_t user_id[16];
    uint8_t page_id[16];
    uint8_t campaign_id[16];
    char event_type[9];
    char ad_type[9];
    int64_t current_ms;
    uint32_t ip;

    record() {
        event_type[0] = '-'; //invalid record
        current_ms = 0;
        ip = 0;
    }

    record(const record& rhs) {
        memcpy(&user_id, &rhs.user_id, 16);
        memcpy(&page_id, &rhs.page_id, 16);
        memcpy(&campaign_id, &rhs.campaign_id, 16);
        memcpy(&event_type, &rhs.event_type, 9);
        memcpy(&ad_type, &rhs.ad_type, 9);
        current_ms = rhs.current_ms;
        ip = rhs.current_ms;
    }

};
//size 78 bytes

union tempHash {
    uint64_t value;
    char buffer[8];
};

struct Tuple {
    Tuple() {
        //campaign_id[0] = '-';//empty campain
        campaign_id = 0;
        timeStamp = std::numeric_limits<std::size_t>::max();
    }
    Tuple(uint64_t pCampaign_id, size_t pTimeStamp) {
        campaign_id = pCampaign_id;
        //memcpy(&campaign_id, pCampaign_id, sizeof(long));
        timeStamp = pTimeStamp;
    }

    //size_t campaign_id;
    uint64_t campaign_id;
    size_t timeStamp;
};
//size 16 Byte

class TupleBuffer{
public:
    TupleBuffer(VerbsConnection& connection, size_t bufferSizeInTuples)
    {
        numberOfTuples = 0;
        maxNumberOfTuples = bufferSizeInTuples;
        send_buffer = connection.allocate_buffer(bufferSizeInTuples * sizeof(Tuple));
        requestToken = connection.create_request_token();
        requestToken->setCompleted(true);
        tups = (Tuple*)(send_buffer->getAddress());
    }

    TupleBuffer(TupleBuffer && other)
        {
            numberOfTuples = other.numberOfTuples;
            maxNumberOfTuples = other.maxNumberOfTuples;

            std::swap(send_buffer, other.send_buffer);
            std::swap(tups, other.tups);
            std::swap(requestToken, other.requestToken);
        }

    bool add(Tuple& tup)
    {
           tups[numberOfTuples] = tup;
           return numberOfTuples == maxNumberOfTuples;
    }

    size_t maxNumberOfTuples;
    Buffer* send_buffer;
    RequestToken * requestToken;
    Tuple* tups;
    size_t numberOfTuples;

//    size_t numberOfTuples;
};


struct ConnectionInfos
{
    ConnectionInfos(){};

    ConnectionInfos(const ConnectionInfos& other)
    {
        records = other.records;
        con = other.con;
        bookKeeping = other.bookKeeping;
//        exitProducer = other.exitProducer;
//        exitConsumer = other.exitConsumer;
    }

    //producer stuff
    record** records;
    VerbsConnection* con;
    std::atomic<size_t>** hashTable;
    tbb::atomic<size_t>* bookKeeping;
};

typedef uint64_t Timestamp;
using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;

void shuffle(record* array, size_t n) {
    if (n > 1) {
        size_t i;
        for (i = 0; i < n - 1; i++) {
            size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            record t = array[j];
            array[j] = array[i];
            array[i] = t;
        }
    }
}



static const std::string events[] = { "view", "click", "purchase" };
void generate(record& data, size_t campaingOffset, uint64_t campaign_lsb,
        uint64_t campaign_msb, size_t event_id) {
    event_id = event_id % 3;

    memcpy(data.campaign_id, &campaign_msb, 8);

    uint64_t campaign_lsbr = campaign_lsb + campaingOffset;
    memcpy(&data.campaign_id[8], &campaign_lsbr, 8);

    const char* str = events[event_id].c_str();
    strcpy(&data.ad_type[0], "banner78");
    strcpy(&data.event_type[0], str);

    auto ts = std::chrono::system_clock::now().time_since_epoch();
    data.current_ms = std::chrono::duration_cast < std::chrono::milliseconds
            > (ts).count();

    data.ip = time(NULL);
}

Timestamp getTimestamp() {
    return std::chrono::duration_cast < NanoSeconds
            > (Clock::now().time_since_epoch()).count();
}

void producer_only(record* records, size_t runCnt, VerbsConnection* con, size_t campaingCnt,
        std::atomic<size_t>** hashTable, size_t windowSizeInSec, tbb::atomic<size_t>* bookKeeper, size_t producerID, size_t rank,
        size_t numberOfNodes)
{
    size_t inputTupsIndex = 0;
    size_t current_window = bookKeeper[0] > bookKeeper[1] ? 0 : 1;
    size_t lastTimeStamp = bookKeeper[current_window];
    size_t windowSwitchCnt = 0;
    size_t htReset = 0;
    size_t disQTuple = 0;
    size_t qualTuple = 0;
    for(size_t i = 0; i < runCnt; i++)
    {
        uint32_t value = *((uint32_t*) records[inputTupsIndex].event_type);
        if (value != 2003134838)
        {
            if(inputTupsIndex < NUMBER_OF_GEN_TUPLE)
                inputTupsIndex++;
            else
                inputTupsIndex = 0;

            disQTuple++;
            continue;
        }
        qualTuple++;

        //tuple qualifies
        size_t timeStamp = time(NULL);
        if (lastTimeStamp != timeStamp && timeStamp % windowSizeInSec == 0)
        {
            if(bookKeeper[current_window].compare_and_swap(timeStamp, lastTimeStamp) == lastTimeStamp)
            {
                    htReset++;
                    #pragma omp critical
                    {
                        cout << "windowing with rank=" << rank << " producerID=" << producerID << " ts=" << timeStamp
                            << " lastts=" << lastTimeStamp << " thread=" << omp_get_thread_num()
                            << " i=" << i << endl;
                    }
                    if(rank != 0)//copy and send result
                    {
                        buffer_threads.push_back(std::make_shared<std::thread>([&con, producerID,
                                                  sharedHT_buffer, outputTable, campaingCnt, current_window, &hashTable, rank] {

                            memcpy(sharedHT_buffer[producerID]->getData(), hashTable[current_window], sizeof(std::atomic<size_t>) * campaingCnt);
                            cout << "send blocking id=" << producerID  << " rank=" << rank << " thread=" << omp_get_thread_num() << endl;
                            con->send_blocking(sharedHT_buffer[producerID]);//send_blocking
                            cout << "send blocking finished " << endl;
                        }));
                    }
                    else if(rank == 0)//this one merges
                    {
                        cout << "merging local stuff for id=" << producerID  << " rank=" << rank << " thread=" << omp_get_thread_num() << endl;
                        #pragma omp parallel for num_threads(20)
                        for(size_t i = 0; i < campaingCnt; i++)
                        {
                            outputTable[i] += hashTable[current_window][i];
                        }

                        cout << "post " << numberOfNodes -1 << " receives" << endl;
                        for(size_t i = 0; i < numberOfNodes -1; i++)
                        {
                            buffer_threads.push_back(std::make_shared<std::thread>([&con, producerID,
                              sharedHT_buffer, outputTable, campaingCnt] {
                                cout << "run buffer thread prodID=" << producerID << endl;
                                ReceiveElement receiveElement;
                                receiveElement.buffer = sharedHT_buffer[producerID];
                                con->post_receive(receiveElement.buffer);
                                while(!con->check_receive(receiveElement))
                                {
    //                                cout << "wait receive producerID=" << producerID << endl;
    //                                sleep(1);
                                }
                                cout << "revceived" << endl;
                                std::atomic<size_t>* tempTable = (std::atomic<size_t>*) sharedHT_buffer[producerID]->getData();

                                #pragma omp parallel for num_threads(20)
                                for(size_t i = 0; i < campaingCnt; i++)
                                {
                                    outputTable[i] += tempTable[i];
                                }
                            }));
                        }//end of for
                    }//end of else
            }//end of if to change
            current_window = current_window == 0 ? 1 : 0;
            lastTimeStamp = timeStamp;
            windowSwitchCnt++;

        }//end of if window is new


        tempHash hashValue;
        hashValue.value = *(((uint64_t*) records[inputTupsIndex].campaign_id) + 1);
        uint64_t bucketPos = (hashValue.value * 789 + 321)% campaingCnt;
        atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));

        if(inputTupsIndex < NUMBER_OF_GEN_TUPLE)
            inputTupsIndex++;
        else
            inputTupsIndex = 0;

    }
//#pragma omp critical
//{
//    stringstream ss;
//    ss << "Thread=" << omp_get_thread_num() << " runCnt="  << runCnt
//            << " disQTuple=" << disQTuple << " qualTuple=" << qualTuple
//            << " windowSwitchCnt=" << windowSwitchCnt
//            << " htreset=" << htReset
//            << " input array=" << &records << endl;
//    cout << ss.str() << endl;
//}
}


record* generateTuplesOneArray(size_t campaingCnt)
{
    std::random_device rd; //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> diss(0, SIZE_MAX);

    size_t randomCnt = NUMBER_OF_GEN_TUPLE / 10;
    size_t* randomNumbers = new size_t[randomCnt];
    std::uniform_int_distribution<size_t> disi(0, campaingCnt);
    for (size_t i = 0; i < randomCnt; i++)
        randomNumbers[i] = disi(gen);

//    record** recs;
    uint64_t campaign_lsb, campaign_msb;
    auto uuid = diss(gen);
    uint8_t* uuid_ptr = reinterpret_cast<uint8_t*>(&uuid);
    memcpy(&campaign_msb, uuid_ptr, 8);
    memcpy(&campaign_lsb, uuid_ptr + 8, 8);
    campaign_lsb &= 0xffffffff00000000;

//    void* pBuffer = numa_alloc_onnode(numberToProduce*NUMBER_OF_GEN_TUPLE*sizeof(record), omp_get_thread_num());
//    record** recs = (record**)pBuffer;

    void* pBuffer = numa_alloc_onnode(NUMBER_OF_GEN_TUPLE*sizeof(record), omp_get_thread_num());
    record* recs = new (pBuffer) record[NUMBER_OF_GEN_TUPLE];
    for (size_t u = 0; u < NUMBER_OF_GEN_TUPLE; u++)
    {
        generate(recs[u], /**campaingOffset*/
                randomNumbers[u % randomCnt], campaign_lsb, campaign_msb, /**eventID*/
                u);
    }
    shuffle(recs, NUMBER_OF_GEN_TUPLE);

    delete[] randomNumbers;
    return recs;
}

//void setupSharedHT(VerbsConnection* connection, size_t campaingCnt, size_t numberOfParticipant, size_t rank)
//{
////    sharedHT_region_token = new RegionToken*[numberOfParticipant+1];
//
//    sharedHT_buffer = new infinity::memory::Buffer*[numberOfParticipant];
//    for(size_t i = 0; i <= numberOfParticipant; i++)
//    {
//        sharedHT_buffer[i] = connection->allocate_buffer(campaingCnt * sizeof(std::atomic<size_t>));
//    }
//
////    infinity::memory::Buffer* tokenbuffer = connection->allocate_buffer((numberOfParticipant+1) * sizeof(RegionToken));
////    if(rank == 0)//reveiver cloud40
////    {
////        for(size_t i = 0; i < numberOfParticipant; i++)
////        {
////           sharedHT_region_token[i] = sharedHT_buffer[i]->createRegionToken();
////           memcpy((RegionToken*)tokenbuffer->getData() + i, sharedHT_region_token[i], sizeof(RegionToken));
////        }
////
////        connection->send_blocking(tokenbuffer);
////        cout << "setupRDMAConsumer finished" << endl;
////    }
////    else//sender cloud43 rank3
////    {
////        connection->post_and_receive_blocking(tokenbuffer);
////        for(size_t i = 0; i < numberOfParticipant; i++)
////        {
////            sharedHT_region_token[i] = new RegionToken();
////            memcpy(sharedHT_region_token[i], (RegionToken*)tokenbuffer->getData() + i, sizeof(RegionToken));
////            cout << "recv region getSizeInBytes=" << sharedHT_region_token[i]->getSizeInBytes() << " getAddress=" << sharedHT_region_token[i]->getAddress()
////                   << " getLocalKey=" << sharedHT_region_token[i]->getLocalKey() << " getRemoteKey=" << sharedHT_region_token[i]->getRemoteKey() << endl;
////        }
////        cout << "received token" << endl;
////    }
//}


ConnectionInfos* setupProducerOnly(VerbsConnection* connection, size_t campaingCnt, size_t rank, size_t numberOfProducer, size_t numberOfHTSlots)
{
    auto outer_thread_id = omp_get_thread_num();
    numa_run_on_node(outer_thread_id);
    numa_set_preferred(outer_thread_id);


    void* b1 = numa_alloc_onnode(100*sizeof(char), outer_thread_id);
    char* buffer_ready_sign = (char*)b1;
    buffer_ready_sign[0] = 't';
    cout << "temp written " << buffer_ready_sign[0] << endl;

    ConnectionInfos* connectInfo = new ConnectionInfos();

    connectInfo->hashTable = new std::atomic<size_t>*[2];
    connectInfo->hashTable[0] = new std::atomic<size_t>[campaingCnt];
    for (size_t i = 0; i < campaingCnt; i++)
       std::atomic_init(&connectInfo->hashTable[0][i], std::size_t(0));

    connectInfo->hashTable[1] = new std::atomic<size_t>[campaingCnt];
    for (size_t i = 0; i < campaingCnt; i++)
       std::atomic_init(&connectInfo->hashTable[1][i], std::size_t(0));

    if(rank == 0)
    {
        outputTable = new std::atomic<size_t>[campaingCnt];
        for (size_t i = 0; i < campaingCnt ; i++)
            std::atomic_init(&outputTable[i], std::size_t(0));
    }

    connectInfo->bookKeeping = new tbb::atomic<size_t>[2];
    connectInfo->bookKeeping[0] = time(NULL);
    connectInfo->bookKeeping[1] = time(NULL) + 2;

    connectInfo->records = new record*[numberOfProducer];
    for(size_t i = 0; i < numberOfProducer; i++)
    {
        connectInfo->records[i] = generateTuplesOneArray(campaingCnt);
    }

    stringstream ss;
    ss  << "Producer Thread #" << outer_thread_id  << ": on CPU " << sched_getcpu() << " nodes=";
    int numa_node = -1;
    get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->hashTable[0], MPOL_F_NODE | MPOL_F_ADDR);
    ss << numa_node << ",";
    ss << endl;
    cout << ss.str() << endl;

    sharedHT_buffer = new infinity::memory::Buffer*[numberOfHTSlots];
    for(size_t i = 0; i <= numberOfHTSlots; i++)
    {
       sharedHT_buffer[i] = connection->allocate_buffer(campaingCnt * sizeof(std::atomic<size_t>));
    }
    connectInfo->con = connection;
    return connectInfo;
}


void printSingleHT(std::atomic<size_t>* hashTable, size_t campaingCnt)
{
    for (size_t i = 0; i < campaingCnt; i++)
    {
        if(hashTable[i] != 0)
            cout << "i=" << i << " cnt=" << hashTable[i] << endl;
    }
    cout << "print down" << endl;
}
void printHT(std::atomic<size_t>** hashTable, size_t campaingCnt, size_t id)
{
    ofstream myfile;
    stringstream fileName;
    fileName << "ht_" << id << ".txt";
    myfile.open (fileName.str());
    myfile << "HT1:" << endl;
    for (size_t i = 0; i < campaingCnt + 1; i++)
    {
        if(hashTable[0][i] != 0)
            myfile << "i=" << i << " cnt=" << hashTable[0][i] << endl;
    }

    myfile << "HT2:" << endl;
    for (size_t i = 0; i < campaingCnt + 1; i++)
    {
        if(hashTable[1][i] != 0)
            myfile << "i=" << i << " cnt=" << hashTable[1][i]<< endl;
    }
    myfile.close();

}
short CorePin(int coreID)
{
  short status=0;
  int nThreads = std::thread::hardware_concurrency();
  //std::cout<<nThreads;
  cpu_set_t set;
  std::cout<<"\nPinning to Core:"<<coreID<<"\n";
  CPU_ZERO(&set);

  if(coreID == -1)
  {
    status=-1;
    std::cout<<"CoreID is -1"<<"\n";
    return status;
  }

  if(coreID > nThreads)
  {
    std::cout<<"Invalid CORE ID"<<"\n";
    return status;
  }

  CPU_SET(coreID,&set);
  if(sched_setaffinity(0, sizeof(cpu_set_t), &set) < 0)
  {
    std::cout<<"Unable to Set Affinity"<<"\n";
    return -1;
  }
  return 1;
}

size_t getNumaNodeFromPtr(void* ptr)
{

    int numa_node1 = -1;
    get_mempolicy(&numa_node1, NULL, 0, ptr, MPOL_F_NODE | MPOL_F_ADDR);

    int status[1];
    status[0]=-1;
    int ret_code = move_pages(0 /*selbuffer_threadsf memory */, 1, &ptr, NULL, status, 0);
    int numa_node2 = status[0];
    assert(numa_node1 == numa_node2);
    return numa_node1;
}

namespace po = boost::program_options;
int main(int argc, char *argv[])
{
    po::options_description desc("Options");

    size_t windowSizeInSeconds = 2;
    size_t campaingCnt = 10000;
    size_t rank = 99;
    size_t numberOfProducer = 2;
    size_t produceCntPerProd = 1;
    size_t numberOfConnections = 1;
    size_t numberOfNodes = 2;
    string ip = "";


    desc.add_options()
        ("help", "Print help messages")
        ("rank", po::value<size_t>(&rank)->default_value(rank), "The rank of the current runtime")
        ("numberOfProducer", po::value<size_t>(&numberOfProducer)->default_value(numberOfProducer), "numberOfProducer")
        ("produceCntPerProd", po::value<size_t>(&produceCntPerProd)->default_value(produceCntPerProd), "produceCntPerProd")
        ("numberOfConnections", po::value<size_t>(&numberOfConnections)->default_value(numberOfConnections), "numberOfConnections")
        ("numberOfNodes", po::value<size_t>(&numberOfNodes)->default_value(numberOfNodes), "numberOfConnections")
        ("ip", po::value<string>(&ip)->default_value(ip), "ip")
        ;

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << "Basic Command Line Parameter " << std::endl
                  << desc << std::endl;
        return 0;
    }

//    size_t tupleProcCnt = bufferProcCnt * bufferSizeInTups * 3;
    MPIHelper::set_rank(rank);

//    assert(rank == 0 || rank == 1);
    std::cout << "Settings:"
//            << " tupleProcCnt=" << tupleProcCnt
            << " Rank=" << rank
            << " genCnt=" << NUMBER_OF_GEN_TUPLE
            << " numberOfProducer=" << numberOfProducer
            << " numberOfConnections=" << numberOfConnections
            << " ip=" << ip
            << endl;


//    assert(numberOfConnections == 1);
    VerbsConnection** connections = new VerbsConnection*[numberOfProducer];
    size_t target_rank = 99;
    if(rank == 0) //cloud41
        target_rank = 10;
    else if(rank == 1)//cloud 42
        target_rank = 0;
    else if(rank == 2)//
        target_rank = 1;
    else if(rank == 3)
        target_rank = 0;
    else
        assert(0);

//    size_t target_rank = rank == 0 ? 1 : 0;
    if(rank == 0 || rank == 1)
    {
        cout << "connecting 0 and 1" << endl;
        SimpleInfoProvider info(target_rank, "mlx5_0", 1, PORT1, ip);//was 3
        connections[0] = new VerbsConnection(&info);
        cout << "connection established rank 0 and 1" << endl;
    }

    if((rank == 0 || rank == 2) && numberOfNodes != 2)
    {
        cout << "connecting 0 and 2" << endl;
        SimpleInfoProvider info(target_rank, "mlx5_1", 1, PORT2, ip);//was 3
        connections[1] = new VerbsConnection(&info);
        cout << "connection established rank 0 and 2" << endl;
    }
    if((rank == 0 || rank == 3) && numberOfNodes != 2)
    {
        cout << "connecting 0 and 3" << endl;
       SimpleInfoProvider info(target_rank, "mlx5_0", 1, PORT3, ip);//was 3
       connections[2] = new VerbsConnection(&info);
       cout << "connection established rank 0 and 3" << endl;
    }


    if(numberOfConnections == 2)
    {
        assert(0);
        SimpleInfoProvider info2(target_rank, "mlx5_2", 1, PORT2, ip);//
        connections[1] = new VerbsConnection(&info2);
    }

    auto nodes = numa_num_configured_nodes();
//    auto cores = numa_num_configured_cpus();
//    auto cores_per_node = cores / nodes;
    omp_set_nested(1);
//    htPtrs = new std::atomic<size_t>*[4];
    ConnectionInfos** conInfos = new ConnectionInfos*[nodes];

    cout << "starting " << nodes << " threads" << endl;
    #pragma omp parallel num_threads(nodes)
    {
        #pragma omp critical
        {
            cout << "thread in critical = " << omp_get_thread_num() << endl;
            if(numberOfConnections == 1)
            {
                if(rank == 0)
                {
                    conInfos[omp_get_thread_num()] = setupProducerOnly(connections[0], campaingCnt, rank, numberOfProducer, 10);
                }
                else
                {
                    conInfos[omp_get_thread_num()] = setupProducerOnly(connections[rank -1], campaingCnt, rank, numberOfProducer, 10);
                }
            }
            else
                assert(0);

        }
        cout << "thread out of critical = " << omp_get_thread_num() << endl;
    }//end of pragma

    do
    {
      cout << '\n' << "Press a key to continue...";
    } while (cin.get() != '\n');

    cout << "start processing " << endl;
    Timestamp begin = getTimestamp();

    #pragma omp parallel num_threads(nodes)//nodes
       {
          auto outer_thread_id = omp_get_thread_num();
          numa_run_on_node(outer_thread_id);
          #pragma omp parallel num_threads(numberOfProducer/nodes)//
          {
             auto inner_thread_id = omp_get_thread_num();

//             #pragma omp critical
//             {
//                 std::cout
//                << "OuterThread=" << outer_thread_id
//                << " InnerThread=" << inner_thread_id
//                << " SumThreadID=" << i
//                << " produceCntPerNode=" << produceCntPerProd
//                << " core: " << sched_getcpu()
//                << " numaNode:" << numa_node_of_cpu(sched_getcpu())
//                << " recordLoc=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->records[inner_thread_id])
//                << " htLoc=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->hashTable[0])
//                << std::endl;
//             }

             record* recs = conInfos[outer_thread_id]->records[inner_thread_id];

             if(rank == 0)
             {
                 producer_only(recs, produceCntPerProd, connections[0], campaingCnt, conInfos[outer_thread_id]->hashTable, windowSizeInSeconds
                                      , conInfos[outer_thread_id]->bookKeeping, rank*2+outer_thread_id, rank, numberOfNodes);
             }
             else
             {
                 producer_only(recs, produceCntPerProd, connections[rank - 1], campaingCnt, conInfos[outer_thread_id]->hashTable, windowSizeInSeconds
                                      , conInfos[outer_thread_id]->bookKeeping, rank*2+outer_thread_id, rank, numberOfNodes);
             }


             assert(outer_thread_id == numa_node_of_cpu(sched_getcpu()));
          }//end of pragma inner threads
       }//end of pragma outer threads

    Timestamp end = getTimestamp();

    size_t sumReadInTuples = numberOfProducer * produceCntPerProd;
    double elapsed_time = double(end - begin) / (1024 * 1024 * 1024);

    stringstream ss;

    ss << " time=" << elapsed_time << "s" << endl;
    ss << " readInputTuples=" << sumReadInTuples  << endl;
    ss << " readInputVolume(MB)=" << sumReadInTuples * sizeof(record) /1024 /1024 << endl;
    ss << " readInputThroughput=" << sumReadInTuples /elapsed_time << endl;
    ss << " readBandWidth MB/s=" << (sumReadInTuples*sizeof(record)/1024/1024)/elapsed_time << endl;

    ss << " ----------------------------------------------" << endl;

    cout << ss.str() << endl;
    for(size_t i = 0; i < buffer_threads.size(); i++)
    {
        buffer_threads[i]->join();
    }
//    printHT(hashTable, campaingCnt);
}
