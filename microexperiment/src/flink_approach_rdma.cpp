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

//#define BUFFER_SIZE 1000
std::atomic<size_t> exitProgram;

using namespace std;
typedef uint64_t Timestamp;
using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;

struct ConnectionObject
{
    char * receive_memory;
    char * send_memory;
    infinity::memory::Buffer* receive_buffer;
    infinity::memory::Buffer* send_buffer;
    std::shared_ptr<RegionToken> remote_receive_token;
    VerbsConnection* connection;
};

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

struct __attribute__((packed)) TupleBuffer {
    TupleBuffer(size_t pBufferSize) {
        pos = 0;
        bufferSize = pBufferSize;
        content = new Tuple[bufferSize];
    }

    bool add(Tuple& tup) {
        content[pos++] = tup;
        return pos == bufferSize;
    }

    Tuple* content;
    size_t pos;
    size_t bufferSize;
};

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

    data.ip = 0x01020304;
}

Timestamp getTimestamp() {
    return std::chrono::duration_cast < NanoSeconds
            > (Clock::now().time_since_epoch()).count();
}

void produce_window_mem(size_t processCnt, record* records,
        ConnectionObject cObj, size_t consumeCnt, size_t prodID, size_t bufferSize) {
    size_t produced = 0;
    size_t disQTuple = 0;
    size_t qualTuple = 0;
    size_t windowSwitchCnt = 0;
    size_t htReset = 0;
    size_t pushCnt = 0;
    size_t lastTimeStamp = 0;
    size_t sendToCons1 = 0;
    size_t sendToCons0 = 0;

    size_t sender = { 0 };

    char* sendBuffer = cObj.send_memory;

    TupleBuffer* tempBuffers = (TupleBuffer*)sendBuffer;
    *(tempBuffers) = TupleBuffer(bufferSize);

    for (size_t i = 0; i < processCnt; i++) {
        uint32_t value = *((uint32_t*) records[i].event_type);
        if (value != 2003134838) {
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

        if (tempBuffers->add(tup)) {
            pushCnt++;
            cObj.connection->write_blocking(cObj.send_buffer, cObj.remote_receive_token.get());
            tempBuffers->pos = 0;
            sender++;
        }
    }
    stringstream ss;
    ss << "Thread=" << omp_get_thread_num() << " prodID=" << prodID
            << " produced=" << produced << " pushCnt=" << pushCnt
            << " disQTuple=" << disQTuple << " qualTuple=" << qualTuple;

    for (size_t i = 0; i < consumeCnt; i++) {
        ss << " send_" << i << "=" << sender;
    }
    cout << ss.str() << endl;

    std::atomic_fetch_add(&exitProgram, size_t(1));
}

void cosume_window_mem(std::atomic<size_t>** hashTable, size_t windowSizeInSec,
        ConnectionObject obj, size_t campaingCnt,
        size_t* consumedRet, size_t consumerID, size_t produceCnt, size_t bufferSize) {
    size_t consumed = 0;
    size_t disQTuple = 0;
    size_t qualTuple = 0;
    size_t windowSwitchCnt = 0;
    size_t htReset = 0;
    size_t lastTimeStamp = 0;
    size_t popCnt = 0;
    Tuple tup;
    TupleBuffer buff(bufferSize);
    bool consume = true;

    while (consume) {
//        while (!queue[consumerID]->empty()) {
//            queue[consumerID]->pop(buff);
            //cout << "Consumer" << consumerID << " consume from to queue " << consumerID << endl;

            popCnt++;
            size_t timeStamp = time(NULL); //seconds elapsed since 00:00 hours, Jan 1, 1970 UTC

            size_t current_window = 0;
            if (lastTimeStamp != timeStamp
                    && timeStamp % windowSizeInSec == 0) {
                //increment to new window
                current_window++;
                windowSwitchCnt++;
                if (hashTable[current_window][campaingCnt] != timeStamp) {
                    htReset++;
                    atomic_store(&hashTable[current_window][campaingCnt], timeStamp);
                    std::fill(hashTable[current_window], hashTable[current_window] + campaingCnt, 0);
                }
                lastTimeStamp = timeStamp;
            }
            for (size_t u = 0; u < bufferSize; u++) {
                //consume one tuple
                uint64_t bucketPos = (buff.content[u].campaign_id * 789 + 321)
                        % campaingCnt;
                atomic_fetch_add(&hashTable[current_window][bucketPos],
                        size_t(1));
                consumed++;
            }
//        } //end of while not empty

        if (std::atomic_load(&exitProgram) == produceCnt)
            consume = false;
    }

    stringstream ss;
    ss << "Thread=" << omp_get_thread_num() << " consumed=" << consumed
            << " popCnt=" << popCnt << " windowSwitchCnt=" << windowSwitchCnt
            << " htreset=" << htReset;
    cout << ss.str() << endl;
    *consumedRet = consumed;
}

//#define SERVER_IP "192.168.5.30"
#define PORT 55355
#define WRITE_SEND_BUFFER_COUNT 20
#define WRITE_RECEIVE_BUFFER_COUNT 20
#define BUFFER_USED_SENDER_DONE 127
#define BUFFER_READY_FLAG 0
#define BUFFER_USED_FLAG 1
#define BUFFER_BEING_PROCESSED_FLAG 2
#define JOIN_WRITE_BUFFER_SIZE 1024*1024*8

void setupRDMAConsumer(VerbsConnection* connection)
{
    std::cout << "Started routine to receive tuples as Consumer" << std::endl;

    std::vector<Buffer*> recv_buffers(WRITE_RECEIVE_BUFFER_COUNT);
    std::vector<RegionToken*> region_tokens(WRITE_RECEIVE_BUFFER_COUNT+1);
    std::vector<std::atomic_char> buffer_ready_sign(WRITE_RECEIVE_BUFFER_COUNT);
    for(auto & r : buffer_ready_sign)
    {
        r = BUFFER_READY_FLAG;
    }

    Buffer * sign_buffer = nullptr;

    for(size_t i = 0; i < WRITE_RECEIVE_BUFFER_COUNT+1; i++)
    {
        if (i < WRITE_RECEIVE_BUFFER_COUNT) {
            recv_buffers[i] = connection->allocate_buffer(JOIN_WRITE_BUFFER_SIZE);
            region_tokens[i] = recv_buffers[i]->createRegionToken();
        } else {
            sign_buffer = connection->register_buffer(buffer_ready_sign.data(), WRITE_RECEIVE_BUFFER_COUNT);
            region_tokens[i] = sign_buffer->createRegionToken();
        }
        memcpy((RegionToken*)recv_buffers[0]->getData() + i, region_tokens[i], sizeof(RegionToken));
    }
    sleep(1);
    std::cout << "PREPARED EVERYTHING FOR RECEIVING!" << std::endl;
    connection->send_blocking(recv_buffers[0]);
}

void copy_received_tokens(const std::vector<StructuredTupleBuffer> &sendBuffers,
        std::vector<RegionToken*> &region_tokens, RegionToken*&sign_token)
{
    for(size_t i = 0; i <= WRITE_RECEIVE_BUFFER_COUNT; i++){
        if ( i < WRITE_RECEIVE_BUFFER_COUNT){
            region_tokens[i] = static_cast<RegionToken*>(malloc(sizeof(RegionToken)));
            memcpy(region_tokens[i], (RegionToken*)sendBuffers[0].send_buffer->getData() + i, sizeof(RegionToken));
        } else {
            sign_token = static_cast<RegionToken*>(malloc(sizeof(RegionToken)));
            memcpy(sign_token, (RegionToken*)sendBuffers[0].send_buffer->getData() + i, sizeof(RegionToken));
        }
    }
}

void setupRDMAProducer(VerbsConnection* connection)
{
    std::cout << "send_matching_tuples_to!" << endl;
    std::vector<RegionToken*> region_tokens(WRITE_RECEIVE_BUFFER_COUNT);

    std::vector<StructuredTupleBuffer> sendBuffers;
    for(size_t i = 0; i < WRITE_SEND_BUFFER_COUNT; i++)
        sendBuffers.emplace_back(StructuredTupleBuffer(*connection, JOIN_WRITE_BUFFER_SIZE));

    std::vector<char> buffer_ready_sign(WRITE_RECEIVE_BUFFER_COUNT, BUFFER_READY_FLAG);
    auto sign_buffer = connection->register_buffer(buffer_ready_sign.data(), WRITE_RECEIVE_BUFFER_COUNT);
    RegionToken* sign_token = nullptr;

    std::cout << "Blocking to receive tokens!" << endl;
    connection->post_and_receive_blocking(sendBuffers[0].send_buffer);
    std::cout << "Received tokens!!\n" << endl;
    copy_received_tokens(sendBuffers, region_tokens, sign_token);

    TRACE2("PREPARED EVERYTHING FOR SENDING!\n");
}



ConnectionObject setupRDMA(size_t numa_node, size_t rank, std::string ip, size_t bufferSize)
{
    pin_to_numa(numa_node);
    MPIHelper::set_rank(rank);
    MPIHelper::set_process_count(2);

    size_t target_rank = rank == 0 ? 1 : 0;
//    (size_t target_rank, u_int16_t device_index, u_int16_t device_Port, u_int16_t connection_port, const std::string & ip);
    SimpleInfoProvider info(target_rank, 3, 1, PORT, ip);
    VerbsConnection* connection = new VerbsConnection(&info);

    char * receive_memory = static_cast<char*>(malloc(bufferSize));
    char * send_memory = static_cast<char*>(malloc(bufferSize));
    memset(receive_memory, 0, bufferSize);
    memset(send_memory, 1, bufferSize);

    auto receive_buffer = connection->register_buffer(receive_memory, bufferSize);
    auto send_buffer = connection->register_buffer(send_memory, bufferSize);

    auto remote_receive_token = connection->exchange_region_tokens(receive_buffer);

//    RequestToken* pRequestToken = connection.create_request_token();
    cout << "waiting on barrier" << endl;
    connection->barrier();
    ConnectionObject retValue;
    retValue.receive_memory = receive_memory;
    retValue.send_memory = send_memory;
    retValue.receive_buffer = receive_buffer;
    retValue.send_buffer = send_buffer;
    retValue.remote_receive_token = remote_receive_token;
    retValue.connection = connection;
}

record** generateTuples(size_t processCnt, size_t num_Producer, size_t campaingCnt)
{
    std::random_device rd; //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> diss(0, SIZE_MAX);

    size_t randomCnt = processCnt / 10;
    size_t* randomNumbers = new size_t[randomCnt];
    std::uniform_int_distribution<size_t> disi(0, campaingCnt);
    for (size_t i = 0; i < randomCnt; i++)
        randomNumbers[i] = disi(gen);

    record** recs;
    uint64_t campaign_lsb, campaign_msb;
    auto uuid = diss(gen);
    uint8_t* uuid_ptr = reinterpret_cast<uint8_t*>(&uuid);
    memcpy(&campaign_msb, uuid_ptr, 8);
    memcpy(&campaign_lsb, uuid_ptr + 8, 8);
    campaign_lsb &= 0xffffffff00000000;

    recs = new record*[num_Producer];
    for (size_t i = 0; i < num_Producer; i++) {
        recs[i] = new record[processCnt];

        for (size_t u = 0; u < processCnt; u++) {
            generate(recs[i][u], /**campaingOffset*/
                    randomNumbers[u % randomCnt], campaign_lsb, campaign_msb, /**eventID*/
                    u);
        }
        shuffle(recs[i], processCnt);
    }
    return recs;
}
int main(int argc, char *argv[])
{
    cout << "usage processCnt bufferSizeInTups rank ip" << endl;
    size_t windowSizeInSeconds = 2;
    //rank 0 producer, rank 1 consumer
    size_t processCnt = std::stoi(argv[1]);
    size_t bufferSizeInKB = std::stoi(argv[2]) * sizeof(Tuple);
    size_t bufferSizeInTups = std::stoi(argv[2]);

    size_t rank = std::stoi(argv[3]);
    std::string ip = "";
    if(rank == 1)
    {
        ip = argv[4];
    }
    assert(rank == 0 || rank == 1);
    std::cout << "processCnt=" << processCnt;
    std::cout << " Rank=" << rank;
    if(rank == 0)
    {
        cout << " Producer" << endl;
    }
    else if(rank == 1)
    {
        cout << " Consumer connecting to " << ip << endl;
    }


    size_t target_rank = rank == 0 ? 1 : 0;
//    (size_t target_rank, u_int16_t device_index, u_int16_t device_Port, u_int16_t connection_port, const std::string & ip);
    SimpleInfoProvider info(target_rank, 3, 1, PORT, ip);
    VerbsConnection* connection = new VerbsConnection(&info);
    if(rank == 0)
    {
        std::cout << "run producer" << endl;
        setupRDMAProducer(connection);
    }
    else
    {
        std::cout << "run consumer" << endl;
        setupRDMAConsumer(connection);
    }
    return 0;


    ConnectionObject connectObj = setupRDMA(0, rank, ip, bufferSizeInKB);

    //fix for the test
    size_t num_Consumer = 1;
    size_t num_Producer = 1;
    const size_t campaingCnt = 10000;


    record** recs = generateTuples(processCnt, num_Producer, campaingCnt);

    //create hash table
    std::atomic<size_t>** hashTable = new std::atomic<size_t>*[2];
    hashTable[0] = new std::atomic<size_t>[campaingCnt + 1];
    for (size_t i = 0; i < campaingCnt + 1; i++)
        std::atomic_init(&hashTable[0][i], std::size_t(0));

    hashTable[1] = new std::atomic<size_t>[campaingCnt + 1];
    for (size_t i = 0; i < campaingCnt + 1; i++)
        std::atomic_init(&hashTable[1][i], std::size_t(0));

    for (size_t i = 0; i < num_Consumer; i++) {
    }

    size_t* consumed = new size_t[num_Consumer];

    Timestamp begin = getTimestamp();

    if(rank == 0)
    {
        produce_window_mem(processCnt, recs[0], connectObj, num_Consumer, 0, bufferSizeInTups);
    }
    else
    {
        cosume_window_mem(hashTable, windowSizeInSeconds, connectObj,
                campaingCnt, &consumed[0],0, num_Producer, bufferSizeInTups);
    }

    Timestamp end = getTimestamp();

    double elapsed_time = double(end - begin) / (1024 * 1024 * 1024);
    size_t consumedOverall = 0;
    for (size_t i = 0; i < num_Consumer; i++) {
        cout << "con " << i << ":" << consumed[i] << endl;
        consumedOverall += consumed[i];
    }
    cout << " time=" << elapsed_time << " produced="
            << num_Producer * processCnt << " throughput="
            << num_Producer * processCnt / elapsed_time << " consumedOverall="
            << consumedOverall << " consumeRarte="
            << consumedOverall / elapsed_time << endl;
}

//    std::vector<Timestamp> measured_times;
//    printf("Starting Measurement!\n");
//    for (size_t iteration = 0; iteration < ITERATIONS; iteration++)
//    {
//        TRACE("Iteration: %d\n", iteration);
//        connection.barrier();
//        auto start_time = TimeTools::now();
//
//        for (size_t i = 0; i < REPETITIONS; i++)
//        {
//            if (rank == 0)
//            {
//                *send_memory = (char) 1;
//                connection.write(send_buffer, remote_receive_token.get());
//                while (((volatile char*) receive_memory)[0] != (char) 1) {
//                }
//                receive_memory[0] = 0;
//            }
//            else
//            {
//                *send_memory = (char) 1;
//                while (((volatile char*) receive_memory)[0] != (char) 1) {
//                }
//                receive_memory[0] = 0;
//                connection.write(send_buffer, remote_receive_token.get());
//            }//end of else
//        }//end of for
//
//        auto end_time = TimeTools::now();
//        measured_times.push_back(((end_time-start_time)/REPETITIONS/2));
//
//    }//end of for iteration
//    printf("Done with Measurement!\n");
