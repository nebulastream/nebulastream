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

//#define BUFFER_SIZE 1000
std::atomic<size_t> exitProgram;
#define PORT 55355
#define WRITE_SEND_BUFFER_COUNT 20
#define WRITE_RECEIVE_BUFFER_COUNT 20
#define BUFFER_USED_SENDER_DONE 127
#define BUFFER_READY_FLAG 0
#define BUFFER_USED_FLAG 1
#define BUFFER_BEING_PROCESSED_FLAG 2
//#define JOIN_WRITE_BUFFER_SIZE 1024*1024*8
//#define DEBUG

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

struct TupleBuffer{
    TupleBuffer(VerbsConnection & connection, size_t bufferSizeInTuples): numberOfTuples(0)
    {
        maxNumberOfTuples = bufferSizeInTuples;
        send_buffer = connection.allocate_buffer(bufferSizeInTuples * sizeof(Tuple));
        requestToken = connection.create_request_token();
        requestToken->setCompleted(true);
        tups = (Tuple*)send_buffer->getAddress();
    }
//    TupleBuffer(void* memory, size_t size)
//    {
//        tups = memory;
//
//    }
    TupleBuffer(TupleBuffer && other)
        :maxNumberOfTuples(other.maxNumberOfTuples)
        , numberOfTuples(other.numberOfTuples)
        {
            std::swap(send_buffer, other.send_buffer);
            std::swap(tups, other.tups);
            std::swap(requestToken, other.requestToken);
        }

    bool add(Tuple& tup)
    {
           tups[numberOfTuples++] = tup;
           return numberOfTuples == maxNumberOfTuples;
    }

    Buffer * send_buffer;
    Tuple* tups;
    RequestToken * requestToken;
    size_t numberOfTuples;
    size_t maxNumberOfTuples;
};
//
//struct __attribute__((packed)) TupleBuffer {
//    TupleBuffer(size_t pBufferSize) {
//        pos = 0;
//        bufferSize = pBufferSize;
//        content = new Tuple[bufferSize];
//    }
//
//    bool add(Tuple& tup) {
//        content[pos++] = tup;
//        return pos == bufferSize;
//    }
//
//    Tuple* content;
//    size_t pos;
//    size_t bufferSize;
//};


std::vector<infinity::memory::Buffer*> recv_buffers(WRITE_RECEIVE_BUFFER_COUNT);
std::vector<infinity::memory::RegionToken*> region_tokens(WRITE_RECEIVE_BUFFER_COUNT+1);
std::vector<char> buffer_ready_sign(WRITE_RECEIVE_BUFFER_COUNT);
//std::vector<std::atomic_char> buffer_ready_sign(WRITE_RECEIVE_BUFFER_COUNT);



//producer stuff
infinity::memory::Buffer* sign_buffer;
RegionToken* sign_token;
std::vector<TupleBuffer> sendBuffers;
using namespace std;
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

    data.ip = 0x01020304;
}

Timestamp getTimestamp() {
    return std::chrono::duration_cast < NanoSeconds
            > (Clock::now().time_since_epoch()).count();
}


void read_sign_buffer(size_t target_rank, Buffer* sign_buffer, RegionToken* sign_token, VerbsConnection* connection)
{
//    TRACE("reading sign_buffer: \n");
    connection->read_blocking(sign_buffer, sign_token);
//    TRACE("sign_buffer: ");
//    for(int i = 0; i < WRITE_RECEIVE_BUFFER_COUNT; i++)
//    {
//        std::cout << (int)buffer_ready_sign[i] << ",";
//    }
//    std::cout << std::endl;
}

size_t produce_window_mem(record* records, size_t genCnt, size_t bufferSize, Tuple* outputBuffer)
{
    size_t bufferIndex = 0;
    size_t inputTupsIndex = 0;
    size_t readTuples = 0;

    while(bufferIndex < bufferSize)
    {
        readTuples++;
        uint32_t value = *((uint32_t*) records[inputTupsIndex].event_type);
        if (value != 2003134838)
        {
            if(inputTupsIndex < genCnt)
                inputTupsIndex++;
            else
                inputTupsIndex = 0;

            continue;
        }

        size_t timeStamp = time(NULL);
        tempHash hashValue;
        hashValue.value = *(((uint64_t*) records[inputTupsIndex].campaign_id) + 1);
        Tuple tup(hashValue.value, timeStamp);
        outputBuffer[bufferIndex++] = tup;

        if(inputTupsIndex < genCnt)
            inputTupsIndex++;
        else
            inputTupsIndex = 0;

    }
    return readTuples;
}

void runProducer(VerbsConnection* connection, record* records, size_t genCnt, size_t bufferSizeInTuples, size_t bufferProcCnt,
        size_t* producesTuples, size_t* producedBuffers, size_t* readInputTuples)
{
    size_t target_rank = 1;
    size_t total_sent_tuples = 0;
    size_t total_buffer_send = 0;
    size_t send_buffer_index = 0;
    size_t readTuples = 0;
    size_t noBufferFreeToSend = 0;
    while(total_buffer_send < bufferProcCnt)
    {
        for(size_t receive_buffer_index = 0; receive_buffer_index < WRITE_RECEIVE_BUFFER_COUNT;
                receive_buffer_index=(receive_buffer_index+1)%WRITE_RECEIVE_BUFFER_COUNT)
        {
            if(receive_buffer_index == 0)
            {
                read_sign_buffer(target_rank, sign_buffer, sign_token, connection);
            }
            if(buffer_ready_sign[receive_buffer_index] == BUFFER_READY_FLAG)
            {
                //this will run until one buffer is filled completely
                readTuples += produce_window_mem(records, genCnt, bufferSizeInTuples, (Tuple*)sendBuffers[send_buffer_index].send_buffer->getData());


                sendBuffers[send_buffer_index].numberOfTuples = bufferSizeInTuples;

                connection->write(sendBuffers[send_buffer_index].send_buffer, region_tokens[receive_buffer_index],
                        sendBuffers[send_buffer_index].requestToken);
#ifdef DEBUG
                cout << "Writing " << sendBuffers[send_buffer_index].numberOfTuples << " tuples on buffer " << receive_buffer_index << endl;
#endif
                total_sent_tuples += sendBuffers[send_buffer_index].numberOfTuples;
                total_buffer_send++;

                if (total_buffer_send < bufferProcCnt+1)//a new buffer will be send next
                {
                    buffer_ready_sign[receive_buffer_index] = BUFFER_USED_FLAG;
                    connection->write(sign_buffer, sign_token, receive_buffer_index, receive_buffer_index, 1);
#ifdef DEBUG
                    cout << "Done writing sign_buffer at index=" << receive_buffer_index << endl;
#endif
                }
                else//finished processing
                {
                    buffer_ready_sign[receive_buffer_index] = BUFFER_USED_SENDER_DONE;
                    connection->write_blocking(sign_buffer, sign_token, receive_buffer_index, receive_buffer_index, 1);
                    total_sent_tuples += sendBuffers[send_buffer_index].numberOfTuples;
                    total_buffer_send++;

                    cout << "Sent last tuples and marked as BUFFER_USED_SENDER_DONE at index=" << receive_buffer_index << endl;
                    break;
                }
                send_buffer_index = (send_buffer_index+1) % WRITE_SEND_BUFFER_COUNT;
            }
            else
            {
                noBufferFreeToSend++;
            }
        }//end of for
    }//end of while
    cout << "Done sending! Sent a total of " << total_sent_tuples << " tuples and " << total_buffer_send << " buffers"
            << " noBufferFreeToSend=" << noBufferFreeToSend << endl;
//    read_sign_buffer(target_rank, sign_buffer, sign_token, connection);

    *producesTuples = total_sent_tuples;
    *producedBuffers = total_buffer_send;
    *readInputTuples = readTuples;

    //    done_with_sending[MPIHelper::get_rank()] = true;
//    auto end_time = TimeTools::now();
//    measured_network_times[MPIHelper::get_process_count() + target_rank] = end_time - start_time;
}

void cosume_window_mem(Tuple* buffer, size_t bufferSizeInTuples, std::atomic<size_t>** hashTable, size_t windowSizeInSec,
        size_t campaingCnt, size_t consumerID, size_t produceCnt, size_t bufferSize) {
    size_t consumed = 0;
    size_t windowSwitchCnt = 0;
    size_t htReset = 0;
    size_t lastTimeStamp = 0;
    Tuple tup;
#ifdef DEBUG
    cout << "Consumer: received buffer with first tuple campaingid=" << buffer[0].campaign_id
                    << " timestamp=" << buffer[0].timeStamp << endl;
#endif
    for(size_t i = 0; i < bufferSizeInTuples; i++)
    {
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

        uint64_t bucketPos = (buffer[i].campaign_id * 789 + 321) % campaingCnt;
        atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
        consumed++;

    }//end of for
//#ifdef DEBUG
    stringstream ss;
    ss << "Thread=" << omp_get_thread_num() << " consumed=" << consumed
            << " windowSwitchCnt=" << windowSwitchCnt
            << " htreset=" << htReset;
    cout << ss.str() << endl;
//#endif
}

void runConsumer(std::atomic<size_t>** hashTable, size_t windowSizeInSec,
        size_t campaingCnt, size_t consumerID, size_t produceCnt, size_t bufferSizeInTuples, size_t* consumedTuples, size_t* consumedBuffers)
{
    size_t total_received_tuples = 0;
    size_t total_received_buffers = 0;
    size_t index = 0;
    size_t noBufferFound = 0;
    cout << "start consumer" << endl;
    std::vector<std::shared_ptr<std::thread>> buffer_threads(WRITE_RECEIVE_BUFFER_COUNT, nullptr);

    while(true)
    {
        index++;
        index %= WRITE_RECEIVE_BUFFER_COUNT;
//        volatile char* c = &buffer_ready_sign[index];
//        cout << "C[" << index << "]vol=" << (int)*c << " c_char=" << (int)(char)buffer_ready_sign[index] << endl;

        if (buffer_ready_sign[index] == BUFFER_USED_FLAG || buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE)
        {
            bool is_done = buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE;

            if(is_done) // this is done so that the loop later doesnt try to process this again
                buffer_ready_sign[index] = BUFFER_READY_FLAG;

            total_received_tuples += bufferSizeInTuples;
            total_received_buffers++;
#ifdef DEBUG
            cout << "Received buffer at index=" << index << endl;
#endif
//            std::future<void> resultFromDB = std::async(std::launch::async, cosume_window_mem, (Tuple*)recv_buffers[index]->getData(), bufferSizeInTuples,
//                                        hashTable, windowSizeInSec, campaingCnt, consumerID, produceCnt, bufferSizeInTuples);

            buffer_threads[index] = std::make_shared<std::thread>([&recv_buffers,bufferSizeInTuples,&hashTable, windowSizeInSec,
                                                                   campaingCnt, consumerID, produceCnt, bufferSizeInTuples, index] {
                cout << "start new thread for consumer" << endl;
                cosume_window_mem((Tuple*)recv_buffers[index]->getData(), bufferSizeInTuples,
                        hashTable, windowSizeInSec, campaingCnt, consumerID, produceCnt, bufferSizeInTuples);
            });


            cosume_window_mem((Tuple*)recv_buffers[index]->getData(), bufferSizeInTuples,
                    hashTable, windowSizeInSec, campaingCnt, consumerID, produceCnt, bufferSizeInTuples);
            buffer_ready_sign[index] = BUFFER_READY_FLAG;
            if(is_done)
                break;
        }
        else
        {
            noBufferFound++;
        }
    }

    for(index = 0; index < WRITE_RECEIVE_BUFFER_COUNT; index++)//check again if some are there
    {
        if (buffer_ready_sign[index] == BUFFER_USED_FLAG) {
            cout << "Check Iter -- Received buffer at index=" << index << endl;

            total_received_tuples += bufferSizeInTuples;
            total_received_buffers++;
            cosume_window_mem((Tuple*)recv_buffers[index]->getData(), bufferSizeInTuples,
                                hashTable, windowSizeInSec, campaingCnt, consumerID, produceCnt, bufferSizeInTuples);
            buffer_ready_sign[index] = BUFFER_READY_FLAG;
        }
        buffer_threads[index].get()->join();
    }

    *consumedTuples = total_received_tuples;
    *consumedBuffers = total_received_buffers;
    cout << "nobufferFound=" << noBufferFound << endl;
//    for (auto & token : region_tokens)
//        delete token;
//    for (auto & buffer : recv_buffers)
//        delete buffer;
}

//#define SERVER_IP "192.168.5.30"

void setupRDMAConsumer(VerbsConnection* connection, size_t bufferSizeInTuples)
{
    std::cout << "Started routine to receive tuples as Consumer" << std::endl;
#ifdef DEBUG
    cout << "buffer size=" << bufferSizeInTuples * sizeof(Tuple) << " first msg size=" << (WRITE_RECEIVE_BUFFER_COUNT+1) * sizeof(RegionToken) << endl;
#endif
    assert(bufferSizeInTuples * sizeof(Tuple) > (WRITE_RECEIVE_BUFFER_COUNT+1) * sizeof(RegionToken));
    for(auto & r : buffer_ready_sign)
    {
        r = BUFFER_READY_FLAG;
    }

//    Buffer * sign_buffer = nullptr;

    for(size_t i = 0; i <= WRITE_RECEIVE_BUFFER_COUNT; i++)
    {
        if (i < WRITE_RECEIVE_BUFFER_COUNT) {
            recv_buffers[i] = connection->allocate_buffer(bufferSizeInTuples * sizeof(Tuple));
            region_tokens[i] = recv_buffers[i]->createRegionToken();
        } else {
//            cout << "copy sign token at pos " << i << endl;
            sign_buffer = connection->register_buffer(buffer_ready_sign.data(), WRITE_RECEIVE_BUFFER_COUNT);
            region_tokens[i] = sign_buffer->createRegionToken();
#ifdef DEBUG
            cout << "sign region getSizeInBytes=" << region_tokens[i]->getSizeInBytes() << " getAddress=" << region_tokens[i]->getAddress()
                    << " getLocalKey=" << region_tokens[i]->getLocalKey() << " getRemoteKey=" << region_tokens[i]->getRemoteKey() << endl;
#endif
        }
        memcpy((RegionToken*)recv_buffers[0]->getData() + i, region_tokens[i], sizeof(RegionToken));
    }
    sleep(1);
    connection->send_blocking(recv_buffers[0]);
    cout << "setupRDMAConsumer finished" << endl;
}

void copy_received_tokens(const std::vector<TupleBuffer> &sendBuffers,
        std::vector<RegionToken*> &region_tokens, RegionToken*&sign_token)
{
    for(size_t i = 0; i <= WRITE_RECEIVE_BUFFER_COUNT; i++){
        if ( i < WRITE_RECEIVE_BUFFER_COUNT){
            region_tokens[i] = static_cast<RegionToken*>(malloc(sizeof(RegionToken)));
            memcpy(region_tokens[i], (RegionToken*)sendBuffers[0].send_buffer->getData() + i, sizeof(RegionToken));
        } else {
            sign_token = static_cast<RegionToken*>(malloc(sizeof(RegionToken)));
            memcpy(sign_token, (RegionToken*)sendBuffers[0].send_buffer->getData() + i, sizeof(RegionToken));
#ifdef DEBUG
            cout << "sign region getSizeInBytes=" << sign_token->getSizeInBytes() << " getAddress=" << sign_token->getAddress()
                                << " getLocalKey=" << sign_token->getLocalKey() << " getRemoteKey=" << sign_token->getRemoteKey() << endl;
#endif
        }
    }
}

void setupRDMAProducer(VerbsConnection* connection, size_t bufferSizeInTuples)
{
//    std::cout << "send_matching_tuples_to!" << endl;

    for(size_t i = 0; i < WRITE_SEND_BUFFER_COUNT; i++)
        sendBuffers.emplace_back(TupleBuffer(*connection, bufferSizeInTuples));

    for(auto & r : buffer_ready_sign)
    {
        r = BUFFER_READY_FLAG;
    }

    sign_buffer = connection->register_buffer(buffer_ready_sign.data(), WRITE_RECEIVE_BUFFER_COUNT);
    sign_token = nullptr;

//    std::cout << "Blocking to receive tokens!" << endl;
    connection->post_and_receive_blocking(sendBuffers[0].send_buffer);
//    std::cout << "Received tokens!!\n" << endl;
    copy_received_tokens(sendBuffers, region_tokens, sign_token);

    cout << "setupRDMAConsumer finished" << endl;
}


record** generateTuples(size_t genCnt, size_t num_Producer, size_t campaingCnt)
{
    std::random_device rd; //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> diss(0, SIZE_MAX);

    size_t randomCnt = genCnt / 10;
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
        recs[i] = new record[genCnt];

        for (size_t u = 0; u < genCnt; u++) {
            generate(recs[i][u], /**campaingOffset*/
                    randomNumbers[u % randomCnt], campaign_lsb, campaign_msb, /**eventID*/
                    u);
        }
        shuffle(recs[i], genCnt);
    }
    return recs;
}

int main(int argc, char *argv[])
{
    cout << "Producer usage: rank bufferProcCnt bufferSizeInTups" << endl;
    cout << "Consumer usage: rank ip bufferSizeInTups" << endl;
    size_t windowSizeInSeconds = 2;


    size_t bufferProcCnt = 0;
    size_t genCnt = 1000000;
    size_t bufferSizeInTups = std::stoi(argv[3]);
    size_t rank = std::stoi(argv[1]);

    MPIHelper::set_rank(rank);
//    MPIHelper::set_process_count(processCnt);

    std::string ip = "";
    if(rank == 0)//producer
    {
        bufferProcCnt = std::stoi(argv[2]);
    }

    if(rank == 1)//consumer
    {
        ip = argv[2];
    }
    assert(rank == 0 || rank == +1);
    std::cout << "bufferProcCnt=" << bufferProcCnt << " genCnt=" << genCnt
            << " Rank=" << rank << " bufferSizeInTups=" << bufferSizeInTups
            << " bufferSizeInKB=" << bufferSizeInTups*sizeof(Tuple)/1024;
    if(rank == 0)
    {
        cout << " Producer" << endl;
    }
    else if(rank == 1)
    {
        cout << " Consumer connecting to " << ip << endl;
    }

    size_t target_rank = rank == 0 ? 1 : 0;
    SimpleInfoProvider info(target_rank, 3, 1, PORT, ip);
    VerbsConnection* connection = new VerbsConnection(&info);
    if(rank == 0)
    {
        std::cout << "run producer" << endl;
        setupRDMAProducer(connection, bufferSizeInTups);
    }
    else
    {
        std::cout << "run consumer" << endl;
        setupRDMAConsumer(connection, bufferSizeInTups);
    }
    //fix for the test
    size_t num_Consumer = 1;
    size_t num_Producer = 1;
    const size_t campaingCnt = 10000;

    record** recs = generateTuples(genCnt, num_Producer, campaingCnt);

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

//    size_t* consumed = new size_t[num_Consumer];

    size_t producesTuples = 0;
    size_t producedBuffers = 0;

    size_t consumedTuples = 0;
    size_t consumedBuffers = 0;

    size_t readInputTuples = 0;
    Timestamp begin = getTimestamp();

    if(rank == 0)
    {
        runProducer(connection, recs[0], genCnt, bufferSizeInTups, bufferProcCnt, &producesTuples, &producedBuffers, &readInputTuples);
    }
    else
    {
        runConsumer(hashTable, windowSizeInSeconds, campaingCnt, 0, num_Producer, bufferSizeInTups, &consumedTuples, &consumedBuffers);
    }

    Timestamp end = getTimestamp();

    double elapsed_time = double(end - begin) / (1024 * 1024 * 1024);
//    size_t consumedOverall = 0;
//    for (size_t i = 0; i < num_Consumer; i++) {
//        cout << "con " << i << ":" << consumed[i] << endl;
//        consumedOverall += consumed[i];
//    }
    stringstream ss;
    ss << " time=" << elapsed_time << "s" << endl;

    ss << " readInputTuples=" << readInputTuples  << endl;
    ss << " readInputVolume(MB)=" << readInputTuples * sizeof(record) /1024 /1024 << endl;
    ss << " readInputThroughput=" << readInputTuples /elapsed_time << endl;
    ss << " readBandWidth MB/s=" << (readInputTuples*sizeof(record)/1024/1024)/elapsed_time << endl;

    ss << " ----------------------------------------------" << endl;

    ss << " produced=" << producesTuples << endl;
    ss << " ProduceThroughput=" << producesTuples / elapsed_time << endl;
    ss << " TransferVolume(MB)=" << producesTuples*sizeof(Tuple)/1024/1024 << endl;
    ss << " TransferBandwidth MB/s=" << (producesTuples*sizeof(Tuple)/1024/1024)/elapsed_time << endl;
    ss << " producesTuples=" << producesTuples << endl;
    ss << " producedBuffers=" << producedBuffers << endl;
    ss << " ----------------------------------------------" << endl;

    ss << " consumedTuples=" << consumedTuples  << endl;
    ss<< " consumedBuffers=" << consumedBuffers  << endl;

    cout << ss.str() << endl;
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
//                while (((x char*) receive_memory)[0] != (char) 1) {
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
//
//ConnectionObject setupRDMA(size_t numa_node, size_t rank, std::string ip, size_t bufferSize)
//{
//    pin_to_numa(numa_node);
//    MPIHelper::set_rank(rank);
//    MPIHelper::set_process_count(2);
//
//    size_t target_rank = rank == 0 ? 1 : 0;
////    (size_t target_rank, u_int16_t device_index, u_int16_t device_Port, u_int16_t connection_port, const std::string & ip);
//    SimpleInfoProvider info(target_rank, 3, 1, PORT, ip);
//    VerbsConnection* connection = new VerbsConnection(&info);
//
//    char * receive_memory = static_cast<char*>(malloc(bufferSize));
//    char * send_memory = static_cast<char*>(malloc(bufferSize));
//    memset(receive_memory, 0, bufferSize);
//    memset(send_memory, 1, bufferSize);
//
//    auto receive_buffer = connection->register_buffer(receive_memory, bufferSize);
//    auto send_buffer = connection->register_buffer(send_memory, bufferSize);
//
//    auto remote_receive_token = connection->exchange_region_tokens(receive_buffer);
//
////    RequestToken* pRequestToken = connection.create_request_token();
//    cout << "waiting on barrier" << endl;
//    connection->barrier();
//    ConnectionObject retValue;
//    retValue.receive_memory = receive_memory;
//    retValue.send_memory = send_memory;
//    retValue.receive_buffer = receive_buffer;
//    retValue.send_buffer = send_buffer;
//    retValue.remote_receive_token = remote_receive_token;
//    retValue.connection = connection;
//}

