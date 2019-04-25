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
//#define JOIN_WRITE_BUFFER_SIZE 1024*1024*8
//#define DEBUG
void printSingleHT(std::atomic<size_t>* hashTable, size_t campaingCnt);

std::atomic<size_t>* exitProducer;
std::atomic<size_t>* exitConsumer;
size_t NUM_SEND_BUFFERS;
//std::atomic<size_t>** htPtrs;
VerbsConnection* sharedHTConnection;
infinity::memory::RegionToken** sharedHT_region_token;
infinity::memory::Buffer** sharedHT_buffer;
char* ht_sign_ready;
infinity::memory::Buffer* ht_sign_ready_buffer;
RegionToken* ready_token;
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
        recv_buffers = other.recv_buffers;
        buffer_ready_sign = other.buffer_ready_sign;
        region_tokens = other.region_tokens;
        sign_buffer = other.sign_buffer;
        sign_token = other.sign_token;
        sendBuffers = other.sendBuffers;
        records = other.records;
        con = other.con;
    }

    //consumer stuff
    infinity::memory::Buffer** recv_buffers;
    char* buffer_ready_sign;

    //both producer and consumer
    infinity::memory::RegionToken** region_tokens;

    //producer stuff
    infinity::memory::Buffer* sign_buffer;//reads the buffer_read from customer into this
    RegionToken* sign_token;//special token for this connection
    TupleBuffer** sendBuffers;
    record** records;
    VerbsConnection* con;
    std::atomic<size_t>** hashTable;
};

//consumer stuff
//infinity::memory::Buffer** recv_buffers;
//char* buffer_ready_sign;
//
////both producer and consumer
//infinity::memory::RegionToken** region_tokens;
//
////producer stuff
//infinity::memory::Buffer* sign_buffer;//reads the buffer_read from customer into this
//RegionToken* sign_token;//special token for this connection
//TupleBuffer** sendBuffers;

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

size_t produce_window_mem(record* records, size_t bufferSize, Tuple* outputBuffer)
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
            if(inputTupsIndex < NUMBER_OF_GEN_TUPLE)
                inputTupsIndex++;
            else
                inputTupsIndex = 0;

            continue;
        }

        size_t timeStamp = time(NULL);
        tempHash hashValue;
        hashValue.value = *(((uint64_t*) records[inputTupsIndex].campaign_id) + 1);

        outputBuffer[bufferIndex].campaign_id = hashValue.value;
        outputBuffer[bufferIndex].timeStamp = timeStamp;

        bufferIndex++;
        if(inputTupsIndex < NUMBER_OF_GEN_TUPLE)
            inputTupsIndex++;
        else
            inputTupsIndex = 0;

    }
    return readTuples;
}

void runProducerOneOnOne(VerbsConnection* connection, record* records, size_t bufferSizeInTuples, size_t bufferProcCnt,
        size_t* producesTuples, size_t* producedBuffers, size_t* readInputTuples, size_t* noFreeEntryFound, size_t startIdx,
        size_t endIdx, size_t numberOfProducer, ConnectionInfos* cInfos, size_t outerThread, size_t connectionID)
{
    size_t total_sent_tuples = 0;
    size_t total_buffer_send = 0;
//    size_t send_buffer_index = 0;
    size_t readTuples = 0;
    size_t noBufferFreeToSend = 0;

    while(total_buffer_send < bufferProcCnt)
    {
        for(size_t receive_buffer_index = startIdx; receive_buffer_index < endIdx && total_buffer_send < bufferProcCnt; receive_buffer_index++)
        {
#ifdef DEBUG
//            cout << "start=" << startIdx << " checks idx=" << receive_buffer_index << endl;
#endif
            if(receive_buffer_index == startIdx)
            {
//                std::lock_guard<std::mutex> lock(m);
//                cout << "read sign buffer" << endl;
//                connection->read_blocking(sign_buffer, sign_token);
//                stringstream ss;
//                ss << "read from startIdx=" << startIdx << " endIdx=" << endIdx << " size=" << endIdx - startIdx << endl;
//                cout << ss.str() << endl;
//                stringstream ss;
//                ss << "BEFORE SIZE=" << cInfos->sign_buffer->getSizeInBytes()
//                                            << " token size= "<< cInfos->sign_token->getSizeInBytes()
//                                            << " idx=" << receive_buffer_index
//                                            << " keyL=" <<  cInfos->sign_token->getLocalKey()
//                                            << " thread=" << omp_get_thread_num()
//                                                            << endl;//                sleep(1);

                connection->read_blocking(cInfos->sign_buffer, cInfos->sign_token, startIdx, startIdx, endIdx - startIdx);
//                ss << "AFTER SIZE=" << cInfos->sign_buffer->getSizeInBytes()
//                                                            << " token size= "<< cInfos->sign_token->getSizeInBytes()
//                                                            << " idx=" << receive_buffer_index
//                                                            << " keyL=" <<  cInfos->sign_token->getLocalKey()
//                                                                            << endl;//
//                ss << "buffer=";
//                for(size_t i = 0; i < NUM_SEND_BUFFERS; i++)
//                    ss << ((char*)cInfos->sign_buffer[receive_buffer_index].getAddress()) +i << " ";
//                cout << ss.str() << endl;

            }
            if(cInfos->buffer_ready_sign[receive_buffer_index] == BUFFER_READY_FLAG)
            {
                //this will run until one buffer is filled completely
                readTuples += produce_window_mem(records, bufferSizeInTuples, cInfos->sendBuffers[receive_buffer_index]->tups);
                cInfos->sendBuffers[receive_buffer_index]->numberOfTuples = bufferSizeInTuples;

//                cout << "using send buffer=" << cInfos->sendBuffers[receive_buffer_index]->numberOfTuples << " "
//                        << cInfos->sendBuffers[receive_buffer_index]->send_buffer->getAddress() << endl;
                connection->write(cInfos->sendBuffers[receive_buffer_index]->send_buffer, cInfos->region_tokens[receive_buffer_index],
                        cInfos->sendBuffers[receive_buffer_index]->requestToken);
#ifdef DEBUG
                cout << "Thread:" << outerThread << "/" << omp_get_thread_num() << "/" << connectionID << " Writing " << cInfos->sendBuffers[receive_buffer_index]->numberOfTuples << " tuples on buffer "
                        << receive_buffer_index << endl;
#endif
                total_sent_tuples += cInfos->sendBuffers[receive_buffer_index]->numberOfTuples;
                total_buffer_send++;

                if (total_buffer_send < bufferProcCnt)//a new buffer will be send next
                {
                    cInfos->buffer_ready_sign[receive_buffer_index] = BUFFER_USED_FLAG;
//                    cout << "sign buffer size=" << cInfos->sign_buffer->getSizeInBytes()
//                            << " token size= "<< cInfos->sign_token->getSizeInBytes()
//                            << " idx=" << receive_buffer_index
//                            << " keyL=" <<  cInfos->sign_token->getLocalKey()
//                                            << endl;//                sleep(1);
                    connection->write_blocking(cInfos->sign_buffer, cInfos->sign_token, receive_buffer_index, receive_buffer_index, 1);
                }
                else//finished processing
                {
                    std::atomic_fetch_add(&exitProducer[outerThread], size_t(1));
                    if(std::atomic_load(&exitProducer[outerThread]) == numberOfProducer/2)
                    {
                        cInfos->buffer_ready_sign[receive_buffer_index] = BUFFER_USED_SENDER_DONE;
                        connection->write_blocking(cInfos->sign_buffer, cInfos->sign_token, receive_buffer_index, receive_buffer_index, 1);
                        cout << "Sent last tuples and marked as BUFFER_USED_SENDER_DONE at index=" << receive_buffer_index << endl;
                    }
                    else
                    {
                        cInfos->buffer_ready_sign[receive_buffer_index] = BUFFER_USED_FLAG;
                        connection->write(cInfos->sign_buffer, cInfos->sign_token, receive_buffer_index, receive_buffer_index, 1);
                    }

                    break;
                }
            }
            else
            {
                noBufferFreeToSend++;
            }
            if(receive_buffer_index +1 > endIdx)
            {
                receive_buffer_index = startIdx;
            }
        }//end of for
    }//end of while
//    cout << "Thread=" << omp_get_thread_num() << " Done sending! Sent a total of " << total_sent_tuples << " tuples and " << total_buffer_send << " buffers"
//            << " noBufferFreeToSend=" << noBufferFreeToSend << " startIDX=" << startIdx << " endIDX=" << endIdx << endl;
#ifdef DEBUG

    #pragma omp critical
             {
                 cout << "Thread:" << outerThread << "/" << omp_get_thread_num() << "/" << connectionID
                         << " producesTuples=" << total_sent_tuples
                         << " producedBuffers=" << total_buffer_send
                         << " readInputTuples=" << readTuples
                         << " noFreeEntryFound=" << noBufferFreeToSend
                         << endl;

             }
#endif
    *producesTuples = total_sent_tuples;
    *producedBuffers = total_buffer_send;
    *readInputTuples = readTuples;
    *noFreeEntryFound = noBufferFreeToSend;
}

size_t runConsumerOneOnOne(Tuple* buffer, size_t bufferSizeInTuples, std::atomic<size_t>** hashTable, size_t windowSizeInSec,
        size_t campaingCnt, size_t consumerID, size_t rank) {
//    return bufferSizeInTuples;
    size_t consumed = 0;
    size_t windowSwitchCnt = 0;
    size_t htReset = 0;
    size_t lastTimeStamp = 0;
    Tuple tup;
    size_t current_window = 0;

//#ifdef DEBUG
//    cout << "Consumer: received buffer with first tuple campaingid=" << buffer[0].campaign_id
//                    << " timestamp=" << buffer[0].timeStamp << endl;
//#endif
    for(size_t i = 0; i < bufferSizeInTuples; i++)
    {
//        cout << " tuple=" << i << " val="<< buffer[i].campaign_id  << endl;
        size_t timeStamp = buffer[i].timeStamp; //seconds elapsed since 00:00 hours, Jan 1, 1970 UTC
        if (lastTimeStamp != timeStamp && timeStamp % windowSizeInSec == 0) {
            //TODO this is not corret atm
            current_window = current_window == 0 ? 1 : 0;
            windowSwitchCnt++;
            if (hashTable[current_window][campaingCnt] != timeStamp)//TODO: replace this with compare and swap
//                if(std::atomic_compare_exchange_weak_explicit(&head,&new_node->next,new_node,std::memory_order_release,std::memory_order_relaxed) == 0)
                {
                    atomic_store(&hashTable[current_window][campaingCnt], timeStamp);
                    htReset++;
//                    cout << "windowing with rank=" << rank << " consumerID=" << consumerID << endl;
                    size_t oldWindow = current_window == 0 ? 1 : 0;
                    if(rank == 3)
                    {
                        //sent to master both hts

                        //copy data to den
//                        cout << "memcp dest=" << sharedHT_buffer[consumerID]->getData() << "src=" << hashTable[oldWindow]
//                           << " size=" << sizeof(std::atomic<size_t>) * campaingCnt << " capa=" << sharedHT_buffer[consumerID]->getSizeInBytes() << endl;
                        while(true)
                        {
                            sharedHTConnection->read_blocking(ht_sign_ready_buffer, ready_token);
//                            cout << "read value is id="<< consumerID << ht_sign_ready[consumerID] << endl;
                            if(ht_sign_ready[consumerID] == BUFFER_READY_FLAG)
                            {
                                break;
                            }
                            else
                            {
                                cout << "not free" << endl;
                                sleep(1);
                            }
                        }

                        memcpy(sharedHT_buffer[consumerID]->getData(), hashTable[oldWindow], sizeof(std::atomic<size_t>) * campaingCnt);

//                        cout << "sent to master node the ht no=" << oldWindow << " toID=" << consumerID << endl;
                        sharedHTConnection->write(sharedHT_buffer[consumerID], sharedHT_region_token[consumerID]);

//                        cout << "set ready flag" << endl;
                        ht_sign_ready[consumerID] = BUFFER_USED_FLAG;//ht_sign_ready
//                        cout << "write ready entry " << endl;
                        sharedHTConnection->write_blocking(ht_sign_ready_buffer, ready_token, consumerID, consumerID, 1);
                    }
                    else if(rank == 1)//this one merges
                    {
//                        cout << "merging local stuff for consumerID=" << consumerID << endl;
                        #pragma omp parallel for
                        for(size_t i = 0; i < campaingCnt; i++)
                        {
//                            cout << "merge i=" << i << " old=" << outputTable[i] << " incold=" << hashTable[oldWindow][i] << endl;
                            outputTable[i] += hashTable[oldWindow][i];
                        }
                        while(true)
                        {
                           if(ht_sign_ready[consumerID] == BUFFER_USED_FLAG)
                           {
//                               cout << " add received ht for id " << consumerID << endl;
                               ht_sign_ready[consumerID] = BUFFER_USED_SENDER_DONE;
                               std::atomic<size_t>* tempTable = (std::atomic<size_t>*) sharedHT_buffer[consumerID]->getData();
                               #pragma omp parallel for
                               for(size_t i = 0; i < campaingCnt; i++)
                               {
//                                   cout << "merge i=" << i << " old=" << outputTable[i] << " inc =" << tempTable[i] << endl;
                                   outputTable[i] += tempTable[i];
                               }
                               ht_sign_ready[consumerID] = BUFFER_READY_FLAG;
                               break;
                            }
                           else if(ht_sign_ready[consumerID] == BUFFER_USED_SENDER_DONE)
                           {
                               cout << "other finished" << endl;
                               break;
                           }
                           else
                           {
                               cout << "wait" << endl;
                           }
                        }
                    }

            }//end of if window
            lastTimeStamp = timeStamp;
        }

        uint64_t bucketPos = (buffer[i].campaign_id * 789 + 321) % campaingCnt;
        atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
        consumed++;

    }//end of for
#ifdef DEBUG
#pragma omp critical
    cout << "Thread=" << omp_get_thread_num() << " consumed=" << consumed
            << " windowSwitchCnt=" << windowSwitchCnt
            << " htreset=" << htReset
            << " consumeID=" << consumerID << endl;
#endif
    return consumed;

}

void runConsumerNew(std::atomic<size_t>** hashTable, size_t windowSizeInSec,
        size_t campaingCnt, size_t consumerID, size_t produceCnt, size_t bufferSizeInTuples, size_t* consumedTuples, size_t* consumedBuffers,
        size_t* consumerNoBufferFound,
        size_t startIdx, size_t endIdx, ConnectionInfos* cInfos, size_t outerThread, size_t rank)
{
    size_t total_received_tuples = 0;
    size_t total_received_buffers = 0;
    size_t index = startIdx;
    size_t noBufferFound = 0;
    size_t consumed = 0;
    while(true)
    {
        if (cInfos->buffer_ready_sign[index] == BUFFER_USED_FLAG || cInfos->buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE)
        {
            bool is_done = cInfos->buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE;

            if(is_done) // this is done so that the loop later doesnt try to process this again
            {
                std::atomic_fetch_add(&exitConsumer[consumerID], size_t(1));
                cInfos->buffer_ready_sign[index] = BUFFER_READY_FLAG;
                cout << "DONE BUFFER FOUND at idx"  << index << endl;
                if(rank == 3)
                {
                    ht_sign_ready[consumerID] = BUFFER_USED_SENDER_DONE;//ht_sign_ready
                    cout << "write finish entry " << endl;
                    sharedHTConnection->write_blocking(ht_sign_ready_buffer, ready_token, consumerID, consumerID, 1);
                }

            }

            total_received_tuples += bufferSizeInTuples;
            total_received_buffers++;
#ifdef DEBUG
            cout << "Thread=" << outerThread << "/" << omp_get_thread_num() << "/" << outerThread<< " Received buffer at index=" << index << endl;
#endif
            consumed += runConsumerOneOnOne((Tuple*)cInfos->recv_buffers[index]->getData(), bufferSizeInTuples,
                    hashTable, windowSizeInSec, campaingCnt, consumerID, rank);

            cInfos->buffer_ready_sign[index] = BUFFER_READY_FLAG;

            if(is_done)
                break;
        }
        else
        {
            noBufferFound++;
        }
        index++;

        if(index > endIdx)
            index = startIdx;

        if(std::atomic_load(&exitConsumer[consumerID]) == 1)
        {
            *consumedTuples = total_received_tuples;
            *consumedBuffers = total_received_buffers;
#ifdef DEBUG

            stringstream ss;
            cout << "befroe out Thread=" << outerThread << "/" << omp_get_thread_num() << " Receiving a total of " << total_received_tuples << " tuples and " << total_received_buffers << " buffers"
                            << " nobufferFound=" << noBufferFound << " startIDX=" << startIdx << " endIDX=" << endIdx << endl;
            cout << ss.str();
#endif
            return;
        }
    }//end of while

    cout << "checking remaining buffers" << endl;
    for(index = startIdx; index < endIdx; index++)//check again if some are there
    {
//        cout << "checking i=" << index << endl;
        if (cInfos->buffer_ready_sign[index] == BUFFER_USED_FLAG) {
            cout << "Check Iter -- Received buffer at index=" << index << endl;

            total_received_tuples += bufferSizeInTuples;
            total_received_buffers++;
            consumed += runConsumerOneOnOne((Tuple*)cInfos->recv_buffers[index]->getData(), bufferSizeInTuples,
                                hashTable, windowSizeInSec, campaingCnt, consumerID, rank);
            cInfos->buffer_ready_sign[index] = BUFFER_READY_FLAG;
        }
    }

    *consumedTuples = consumed;
    *consumedBuffers = total_received_buffers;
    *consumerNoBufferFound = noBufferFound;
    cout << "Thread=" << omp_get_thread_num() << " Done sending! Receiving a total of " << total_received_tuples << " tuples and " << total_received_buffers << " buffers"
                << " nobufferFound=" << noBufferFound << " startIDX=" << startIdx << " endIDX=" << endIdx << endl;
}

void setupSharedHT(VerbsConnection* connection, size_t campaingCnt, size_t numberOfParticipant, size_t rank)
{
    sharedHT_region_token = new RegionToken*[numberOfParticipant+1];

    sharedHT_buffer = new infinity::memory::Buffer*[numberOfParticipant];
    for(size_t i = 0; i <= numberOfParticipant; i++)
    {
        sharedHT_buffer[i] = connection->allocate_buffer(campaingCnt * sizeof(std::atomic<size_t>));
    }

    infinity::memory::Buffer* tokenbuffer = connection->allocate_buffer((numberOfParticipant+1) * sizeof(RegionToken));

    ht_sign_ready = new char[numberOfParticipant];
    for(size_t i = 0; i < numberOfParticipant; i++)
    {
        ht_sign_ready[i] = BUFFER_READY_FLAG;
    }
    ht_sign_ready_buffer = connection->register_buffer(ht_sign_ready, numberOfParticipant);

    if(rank == 1)//reveiver cloud40
    {
        for(size_t i = 0; i <= numberOfParticipant; i++)
           {
               if (i < numberOfParticipant)
               {
                   sharedHT_region_token[i] = sharedHT_buffer[i]->createRegionToken();
               }
               else
               {
                   sharedHT_region_token[i] = ht_sign_ready_buffer->createRegionToken();
               }

               memcpy((RegionToken*)tokenbuffer->getData() + i, sharedHT_region_token[i], sizeof(RegionToken));
           }

        connection->send_blocking(tokenbuffer);
        cout << "setupRDMAConsumer finished" << endl;
    }
    else//sender cloud43 rank3
    {
        connection->post_and_receive_blocking(tokenbuffer);
        for(size_t i = 0; i <= numberOfParticipant; i++)
        {
            if (i < numberOfParticipant)
            {
                sharedHT_region_token[i] = new RegionToken();
                memcpy(sharedHT_region_token[i], (RegionToken*)tokenbuffer->getData() + i, sizeof(RegionToken));
                cout << "recv region getSizeInBytes=" << sharedHT_region_token[i]->getSizeInBytes() << " getAddress=" << sharedHT_region_token[i]->getAddress()
                                                           << " getLocalKey=" << sharedHT_region_token[i]->getLocalKey() << " getRemoteKey=" << sharedHT_region_token[i]->getRemoteKey() << endl;
            }
            else
            {
                ready_token = new RegionToken();
                memcpy(ready_token, (RegionToken*)tokenbuffer->getData() + i, sizeof(RegionToken));
                cout << "sign token region getSizeInBytes=" << ready_token->getSizeInBytes() << " getAddress=" << ready_token->getAddress()
                                          << " getLocalKey=" << ready_token->getLocalKey() << " getRemoteKey=" << ready_token->getRemoteKey() << endl;
            }
        }
        cout << "received token" << endl;
    }
}


ConnectionInfos* setupRDMAConsumer(VerbsConnection* connection, size_t bufferSizeInTups, size_t campaingCnt)
{
    ConnectionInfos* connectInfo = new ConnectionInfos();

    auto outer_thread_id = omp_get_thread_num();
    numa_run_on_node(outer_thread_id);
    numa_set_preferred(outer_thread_id);

    void* b1 = numa_alloc_onnode(NUM_SEND_BUFFERS*sizeof(char), outer_thread_id);
    connectInfo->buffer_ready_sign = (char*)b1;
    for(size_t i = 0; i < NUM_SEND_BUFFERS; i++)
    {
        connectInfo->buffer_ready_sign[i] = BUFFER_READY_FLAG;
    }

    void* b2 = numa_alloc_onnode((NUM_SEND_BUFFERS+1)*sizeof(RegionToken), outer_thread_id);
    connectInfo->region_tokens = (infinity::memory::RegionToken**)b2;


    void* pBuffer = numa_alloc_onnode(NUM_SEND_BUFFERS*sizeof(Buffer), outer_thread_id);
    connectInfo->recv_buffers = (infinity::memory::Buffer**)pBuffer;

    infinity::memory::Buffer* tokenbuffer = connection->allocate_buffer((NUM_SEND_BUFFERS+1) * sizeof(RegionToken));

    connectInfo->sign_token = nullptr;


    for(size_t i = 0; i <= NUM_SEND_BUFFERS; i++)
    {
        if (i < NUM_SEND_BUFFERS) {
            connectInfo->recv_buffers[i] = connection->allocate_buffer(bufferSizeInTups * sizeof(Tuple));
            connectInfo->region_tokens[i] = connectInfo->recv_buffers[i]->createRegionToken();

//            if(outer_thread_id == 0)
//                s2 << "i=" << i << "recv region getSizeInBytes=" << connectInfo->recv_buffers[i]->getSizeInBytes() << " getAddress=" << connectInfo->recv_buffers[i]->getAddress()
//                                       << " getLocalKey=" << connectInfo->recv_buffers[i]->getLocalKey() << " getRemoteKey=" << connectInfo->recv_buffers[i]->getRemoteKey() << endl;
        } else {
//            cout << "copy sign token at pos " << i << endl;
            connectInfo->sign_buffer = connection->register_buffer(connectInfo->buffer_ready_sign, NUM_SEND_BUFFERS);

            connectInfo->region_tokens[i] = connectInfo->sign_buffer->createRegionToken();

//            if(outer_thread_id == 0)
//                s2 << "i=" << i << "sign region getSizeInBytes=" << connectInfo->sign_buffer->getSizeInBytes() << " getAddress=" << connectInfo->sign_buffer->getAddress()
//                                       << " getLocalKey=" << connectInfo->sign_buffer->getLocalKey() << " getRemoteKey=" << connectInfo->sign_buffer->getRemoteKey() << endl;

        }
        memcpy((RegionToken*)tokenbuffer->getData() + i, connectInfo->region_tokens[i], sizeof(RegionToken));
    }

//    if(outer_thread_id == 0)
//    {
//        cout << "0=" << s2.str() << endl;
//    }

    connection->send_blocking(tokenbuffer);
    cout << "setupRDMAConsumer finished" << endl;

    stringstream ss;
    ss  << "Consumer Thread #" << omp_get_thread_num()  << ": on CPU " << sched_getcpu() << " nodes=";
    int numa_node = -1;
    get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->recv_buffers, MPOL_F_NODE | MPOL_F_ADDR);
    ss << numa_node << ",";
    get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->recv_buffers[0]->getData(), MPOL_F_NODE | MPOL_F_ADDR);
    ss << numa_node << ",";
    get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->buffer_ready_sign, MPOL_F_NODE | MPOL_F_ADDR);
    ss << numa_node << ",";
    get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->region_tokens, MPOL_F_NODE | MPOL_F_ADDR);
    ss << numa_node << ",";
    get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->region_tokens[0]->getAddress(), MPOL_F_NODE | MPOL_F_ADDR);
        ss << numa_node << ",";
    get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->sign_token, MPOL_F_NODE | MPOL_F_ADDR);
    ss << numa_node << ",";
    void * ptr_to_check = connectInfo->recv_buffers;
    int status[1];
    status[0]=-1;
    int ret_code = move_pages(0 /*self memory */, 1, &ptr_to_check, NULL, status, 0);
    ss << status[0] << ",";
    cout << ss.str() << endl;


    connectInfo->hashTable = new std::atomic<size_t>*[2];
    connectInfo->hashTable[0] = new std::atomic<size_t>[campaingCnt + 1];
    for (size_t i = 0; i < campaingCnt + 1; i++)
       std::atomic_init(&connectInfo->hashTable[0][i], std::size_t(0));

    connectInfo->hashTable[1] = new std::atomic<size_t>[campaingCnt + 1];
    for (size_t i = 0; i < campaingCnt + 1; i++)
       std::atomic_init(&connectInfo->hashTable[1][i], std::size_t(0));

    outputTable = new std::atomic<size_t>[campaingCnt];
    for (size_t i = 0; i < campaingCnt ; i++)
        std::atomic_init(&outputTable[i], std::size_t(0));

//    htPtrs[outer_thread_id] = hashTable[0];
//    htPtrs[outer_thread_id*2] = hashTable[1];
    return connectInfo;
}


ConnectionInfos* setupRDMAProducer(VerbsConnection* connection, size_t bufferSizeInTups)
{
    ConnectionInfos* connectInfo = new ConnectionInfos();
    auto outer_thread_id = omp_get_thread_num();
    numa_run_on_node(outer_thread_id);
    numa_set_preferred(outer_thread_id);

    void* pBuffer = numa_alloc_onnode(NUM_SEND_BUFFERS*sizeof(TupleBuffer), outer_thread_id);
    connectInfo->sendBuffers = (TupleBuffer**)pBuffer;

    for(size_t i = 0; i < NUM_SEND_BUFFERS; ++i)
    {
//        connectInfo->sendBuffers[i] = new (connectInfo->sendBuffers + i) TupleBuffer(*connection, bufferSizeInTups);,
        connectInfo->sendBuffers[i] = new TupleBuffer(*connection, bufferSizeInTups);//TODO:not sure if this is right
    }

    void* b3 = numa_alloc_onnode(NUM_SEND_BUFFERS*sizeof(char), outer_thread_id);
    connectInfo->buffer_ready_sign = (char*)b3;
    for(size_t i = 0; i < NUM_SEND_BUFFERS; i++)
    {
        connectInfo->buffer_ready_sign[i] = BUFFER_READY_FLAG;
    }

    connectInfo->sign_buffer = connection->register_buffer(connectInfo->buffer_ready_sign, NUM_SEND_BUFFERS);
    cout << "prod sign buffer size=" << connectInfo->sign_buffer->getSizeInBytes() << endl;
    infinity::memory::Buffer* tokenbuffer = connection->allocate_buffer((NUM_SEND_BUFFERS+1) * sizeof(RegionToken));

    void* b2 = numa_alloc_onnode((NUM_SEND_BUFFERS+1)*sizeof(RegionToken), outer_thread_id);
    connectInfo->region_tokens = (infinity::memory::RegionToken**)b2;

    connection->post_and_receive_blocking(tokenbuffer);
    stringstream s2;
    for(size_t i = 0; i <= NUM_SEND_BUFFERS; i++)
    {
        if ( i < NUM_SEND_BUFFERS){
            connectInfo->region_tokens[i] = static_cast<RegionToken*>(numa_alloc_onnode(sizeof(RegionToken), outer_thread_id));
            memcpy(connectInfo->region_tokens[i], (RegionToken*)tokenbuffer->getData() + i, sizeof(RegionToken));
//            if(outer_thread_id == 0)
//                s2 << "region getSizeInBytes=" << connectInfo->region_tokens[i]->getSizeInBytes() << " getAddress=" << connectInfo->region_tokens[i]->getAddress()
//                    << " getLocalKey=" << connectInfo->region_tokens[i]->getLocalKey() << " getRemoteKey=" << connectInfo->region_tokens[i]->getRemoteKey() << endl;
        }
        else {
            connectInfo->sign_token = static_cast<RegionToken*>(numa_alloc_onnode(sizeof(RegionToken), outer_thread_id));
            memcpy(connectInfo->sign_token, (RegionToken*)tokenbuffer->getData() + i, sizeof(RegionToken));

//            if(outer_thread_id == 0)
//                s2 << " SIGN LOCALregion getSizeInBytes=" << connectInfo->sign_token->getSizeInBytes() << " getAddress=" << connectInfo->sign_token->getAddress()
//                    << " getLocalKey=" << connectInfo->sign_token->getLocalKey() << " getRemoteKey=" << connectInfo->sign_token->getRemoteKey() << endl;
        }
    }
//    if(outer_thread_id == 0)
//   {
//       cout << "0=" << s2.str() << endl;
//   }
   stringstream ss;
   ss  << "Producer Thread #" << outer_thread_id  << ": on CPU " << sched_getcpu() << " nodes=";
   int numa_node = -1;
   get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->sendBuffers, MPOL_F_NODE | MPOL_F_ADDR);
   ss << numa_node << ",";
   get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->sendBuffers[1]->send_buffer, MPOL_F_NODE | MPOL_F_ADDR);
   ss << numa_node << ",";
   get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->buffer_ready_sign, MPOL_F_NODE | MPOL_F_ADDR);
   ss << numa_node << ",";
   get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->region_tokens, MPOL_F_NODE | MPOL_F_ADDR);
   ss << numa_node << ",";
   get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->sign_buffer, MPOL_F_NODE | MPOL_F_ADDR);
   ss << numa_node << ",";
   get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->sign_buffer[0].getData(), MPOL_F_NODE | MPOL_F_ADDR);
   ss << numa_node << ",";
   get_mempolicy(&numa_node, NULL, 0, (void*)connectInfo->sign_token, MPOL_F_NODE | MPOL_F_ADDR);
   ss << numa_node << ",";
   void * ptr_to_check = connectInfo->sendBuffers;
   int status[1];
   status[0]=-1;
   int ret_code = move_pages(0 /*self memory */, 1, &ptr_to_check, NULL, status, 0);
   ss << status[0] << endl;
   cout << ss.str() << endl;

   return connectInfo;

}


record* generateTuplesOneArray(size_t num_Producer, size_t campaingCnt)
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

//    recs = new record*[num_Producer];
//    for (size_t i = 0; i < num_Producer; i++) {
//        void* ptr = numa_alloc_onnode(NUMBER_OF_GEN_TUPLE*sizeof(record), omp_get_thread_num());
//        record* ptrRec = (record*)ptr;
//        recs[i] = new (ptrRec + i) record();
//
////        recs[i] = new record[NUMBER_OF_GEN_TUPLE];
//
//        for (size_t u = 0; u < NUMBER_OF_GEN_TUPLE; u++) {
//            generate(recs[i][u], /**campaingOffset*/
//                    randomNumbers[u % randomCnt], campaign_lsb, campaign_msb, /**eventID*/
//                    u);
//        }
//        shuffle(recs[i], NUMBER_OF_GEN_TUPLE);
//    }

    delete[] randomNumbers;
    return recs;
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
    int ret_code = move_pages(0 /*self memory */, 1, &ptr, NULL, status, 0);
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

    exitProducer = new std::atomic<size_t>[2];
    exitConsumer = new std::atomic<size_t>[2];
    exitProducer[0] = exitProducer[1] = exitConsumer[0] = exitConsumer[1] = 0;

    size_t rank = 99;
    size_t numberOfProducer = 2;
    size_t numberOfConsumer = 1;
    size_t bufferProcCnt = 0;
    size_t bufferSizeInTups = 0;
    size_t numberOfConnections = 1;
    NUM_SEND_BUFFERS = 0;
    size_t numberOfNodes = 4;
    string ip = "";


    desc.add_options()
        ("help", "Print help messages")
        ("rank", po::value<size_t>(&rank)->default_value(rank), "The rank of the current runtime")
        ("numberOfProducer", po::value<size_t>(&numberOfProducer)->default_value(numberOfProducer), "numberOfProducer")
        ("numberOfConsumer", po::value<size_t>(&numberOfConsumer)->default_value(numberOfConsumer), "numberOfConsumer")
        ("bufferProcCnt", po::value<size_t>(&bufferProcCnt)->default_value(bufferProcCnt), "bufferProcCnt")
        ("bufferSizeInTups", po::value<size_t>(&bufferSizeInTups)->default_value(bufferSizeInTups), "bufferSizeInTups")
        ("sendBuffers", po::value<size_t>(&NUM_SEND_BUFFERS)->default_value(NUM_SEND_BUFFERS), "sendBuffers")
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
            << " bufferProcCnt=" << bufferProcCnt
//            << " tupleProcCnt=" << tupleProcCnt
            << " Rank=" << rank
            << " genCnt=" << NUMBER_OF_GEN_TUPLE
            << " bufferSizeInTups=" << bufferSizeInTups
            << " bufferSizeInKB=" << bufferSizeInTups * sizeof(Tuple) / 1024
            << " numberOfSendBuffer=" << NUM_SEND_BUFFERS
            << " numberOfProducer=" << numberOfProducer
            << " numberOfConsumer=" << numberOfConsumer
            << " numberOfConnections=" << numberOfConnections
            << " ip=" << ip
            << endl;


//    assert(numberOfConnections == 1);
    VerbsConnection** connections = new VerbsConnection*[numberOfConnections];
    size_t target_rank = 99;
    if(rank == 0) //cloud41
        target_rank = 1;
    else if(rank == 1)//cloud 42
        target_rank = 0;
    else if(rank == 2)//
        target_rank = 3;
    else if(rank == 3)
        target_rank = 2;
    else
        assert(0);

//    size_t target_rank = rank == 0 ? 1 : 0;
    SimpleInfoProvider info1(target_rank, "mlx5_0", 1, PORT1, ip);//was 3
    connections[0] = new VerbsConnection(&info1);
    cout << "first connection established" << endl;
    if(numberOfConnections == 2)
    {
        SimpleInfoProvider info2(target_rank, "mlx5_2", 1, PORT2, ip);//

        connections[1] = new VerbsConnection(&info2);
    }
    auto nodes = numa_num_configured_nodes();
//    auto cores = numa_num_configured_cpus();
//    auto cores_per_node = cores / nodes;
    omp_set_nested(1);
//    htPtrs = new std::atomic<size_t>*[4];
    ConnectionInfos** conInfos = new ConnectionInfos*[nodes];
    if(rank % 2 == 0)//producer
    {
        cout << "starting " << nodes << " threads" << endl;
        #pragma omp parallel num_threads(nodes)
        {
            #pragma omp critical
            {
                cout << "thread in critical = " << omp_get_thread_num() << endl;
                if(numberOfConnections == 1)
                {
                    conInfos[omp_get_thread_num()] = setupRDMAProducer(connections[0], bufferSizeInTups);
                    conInfos[omp_get_thread_num()]->con = connections[0];
                }
                else if(numberOfConnections == 2)
                {
                    conInfos[omp_get_thread_num()] = setupRDMAProducer(connections[omp_get_thread_num()], bufferSizeInTups);
                    conInfos[omp_get_thread_num()]->con = connections[omp_get_thread_num()];
                }
                else
                    assert(0);

                conInfos[omp_get_thread_num()]->records = new record*[numberOfProducer];
                for(size_t i = 0; i < numberOfProducer; i++)
                {
                    conInfos[omp_get_thread_num()]->records[i] = generateTuplesOneArray(numberOfProducer, campaingCnt);
                }
            }
            cout << "thread out of critical = " << omp_get_thread_num() << endl;
        }//end of pragma
    }
    else//consumer
    {
        cout << "starting " << nodes << " threads" << endl;
        #pragma omp parallel num_threads(nodes)
        {
            #pragma omp critical
            {
                cout << "thread in critical = " << omp_get_thread_num() << endl;
                if(numberOfConnections == 1)
                {
                    conInfos[omp_get_thread_num()] = setupRDMAConsumer(connections[0], bufferSizeInTups, campaingCnt);
                    conInfos[omp_get_thread_num()]->con = connections[0];
                }
                else if(numberOfConnections == 2)
                {
                    conInfos[omp_get_thread_num()] = setupRDMAConsumer(connections[omp_get_thread_num()], bufferSizeInTups, campaingCnt);
                    conInfos[omp_get_thread_num()]->con = connections[omp_get_thread_num()];
                }
                else
                    assert(0);
            }//end of critical
            cout << "thread out of critical = " << omp_get_thread_num() << endl;
        }
        if(numberOfNodes == 4)
        {
            sleep(2);
            if(rank == 1 || rank == 3)
            {
                size_t targetR = rank == 1 ? 3 : 1;
                cout << "establish connection for shared ht rank=" << rank << " targetRank=" << targetR << endl;
                //host cloud 42 rank 2, client cloud43  rank 4 mlx5_3
                SimpleInfoProvider info(targetR, "mlx5_1", 1, PORT3, "192.168.5.10");
                sharedHTConnection = new VerbsConnection(&info);
                setupSharedHT(sharedHTConnection, campaingCnt, 2, rank);
            }

        }
    }//end of else
    size_t producesTuples[nodes][numberOfProducer/nodes] = {0};
    size_t producedBuffers[nodes][numberOfProducer/nodes] = {0};
    size_t noFreeEntryFound[nodes][numberOfProducer/nodes] = {0};
    size_t consumedTuples[nodes][numberOfConsumer/nodes] = {0};
    size_t consumedBuffers[nodes][numberOfConsumer/nodes] = {0};
    size_t consumerNoBufferFound[nodes][numberOfConsumer/nodes] = {0};
    size_t readInputTuples[nodes][numberOfProducer/nodes] = {0};

    infinity::memory::Buffer* finishBuffer = connections[0]->allocate_buffer(1);

    Timestamp begin = getTimestamp();
    if(rank % 2 == 0)
    {
    #pragma omp parallel num_threads(nodes)//nodes
       {
          auto outer_thread_id = omp_get_thread_num();
          numa_run_on_node(outer_thread_id);
          #pragma omp parallel num_threads(numberOfProducer/nodes)//
          {
             auto inner_thread_id = omp_get_thread_num();
             size_t i = inner_thread_id;
             size_t share = NUM_SEND_BUFFERS/(numberOfProducer/nodes);
             size_t startIdx = inner_thread_id* share;
             size_t endIdx = (inner_thread_id+1)*share;
             record* recs = conInfos[outer_thread_id]->records[inner_thread_id];

#ifdef DEBUG
             #pragma omp critical
             {
                 std::cout
                << "OuterThread=" << outer_thread_id
                << " InnerThread=" << inner_thread_id
                << " SumThreadID=" << i
                << " core: " << sched_getcpu()
                << " numaNode:" << numa_node_of_cpu(sched_getcpu())
                << " sendBufferLocation=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->sendBuffers)
                << " sendBufferDataLocation=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->sendBuffers[0]->send_buffer)
                << " inputdata numaNode=" << getNumaNodeFromPtr(recs)
                << " start=" << startIdx
                << " endidx=" << endIdx
                << " share=" << share
                << std::endl;
             }
#endif
             VerbsConnection* con;
             size_t connectID;
             if(numberOfConnections == 1)
             {
                 con = connections[0];
                 connectID = 0;
             }
             else
             {
                 con = connections[outer_thread_id];
                 connectID = outer_thread_id;
             }

             runProducerOneOnOne(con, recs, bufferSizeInTups, bufferProcCnt/numberOfProducer, &producesTuples[outer_thread_id][i],
                     &producedBuffers[outer_thread_id][i], &readInputTuples[outer_thread_id][i], &noFreeEntryFound[outer_thread_id][i], startIdx, endIdx,
                     numberOfProducer, conInfos[outer_thread_id], outer_thread_id, connectID);

             assert(outer_thread_id == numa_node_of_cpu(sched_getcpu()));
          }
       }
       cout << "producer finished ... waiting for consumer to finish " << getTimestamp() << endl;
       connections[0]->post_and_receive_blocking(finishBuffer);
       cout << "got finish buffer, finished execution " << getTimestamp()<< endl;
    }
    else
    {
#pragma omp parallel num_threads(nodes)
       {
          auto outer_thread_id = omp_get_thread_num();
          numa_run_on_node(outer_thread_id);
          #pragma omp parallel num_threads(numberOfConsumer/nodes)
          {
             auto inner_thread_id = omp_get_thread_num();
             size_t i = inner_thread_id;
             size_t share = NUM_SEND_BUFFERS/(numberOfConsumer/nodes);
             size_t startIdx = i* share;
             size_t endIdx = (i+1)*share;
             if(i == numberOfConsumer -1)
             {
                 endIdx = NUM_SEND_BUFFERS;
             }
//#ifdef DEBUG
             #pragma omp critical
             {
             std::cout
                << "OuterThread=" << outer_thread_id
                << " InnerThread=" << inner_thread_id
                << " SumThreadID=" << i
                << " core: " << sched_getcpu()
                << " numaNode:" << numa_node_of_cpu(sched_getcpu())
                << " receiveBufferLocation=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->recv_buffers)
                << " receiveBufferDataLocation=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->recv_buffers[0]->getData())
                << " start=" << startIdx
                << " endidx=" << endIdx
                << " share=" << share
                << std::endl;
             }
//#endif
             runConsumerNew(conInfos[outer_thread_id]->hashTable, windowSizeInSeconds, campaingCnt, outer_thread_id, numberOfProducer , bufferSizeInTups,
                     &consumedTuples[outer_thread_id][i], &consumedBuffers[outer_thread_id][i], &consumerNoBufferFound[outer_thread_id][i], startIdx,
                     endIdx, conInfos[outer_thread_id], outer_thread_id, rank);
//             printSingleHT(conInfos[outer_thread_id]->hashTable[0], campaingCnt);
//                 printHT(conInfos[outer_thread_id]->hashTable, campaingCnt, outer_thread_id);
          }
       }
       cout << "finished, sending finish buffer " << getTimestamp() << endl;
       connections[0]->send_blocking(finishBuffer);
       cout << "buffer sending finished, shutdown "<< getTimestamp() << endl;

    }

    Timestamp end = getTimestamp();

    size_t sumProducedTuples = 0;
    size_t sumProducedBuffer = 0;
    size_t sumReadInTuples = 0;
    size_t sumNoFreeEntry = 0;
    size_t sumConsumedTuples = 0;
    size_t sumConsumedBuffer = 0;
    size_t sumNoBuffer = 0;

    for(size_t n = 0; n < nodes; n++)
    {
        for(size_t i = 0; i < numberOfProducer/nodes; i++)
        {
            sumProducedTuples += producesTuples[n][i];
            sumProducedBuffer += producedBuffers[n][i];
            sumReadInTuples += readInputTuples[n][i];
            sumNoFreeEntry += noFreeEntryFound[n][i];
        }

        for(size_t i = 0; i < numberOfConsumer/nodes; i++)
        {
            sumConsumedTuples += consumedTuples[n][i];
            sumConsumedBuffer += consumedBuffers[n][i];
            sumNoBuffer += consumerNoBufferFound[n][i];
        }
    }



    double elapsed_time = double(end - begin) / (1024 * 1024 * 1024);

    stringstream ss;

    ss << " time=" << elapsed_time << "s" << endl;

    ss << " readInputTuples=" << sumReadInTuples  << endl;
    ss << " readInputVolume(MB)=" << sumReadInTuples * sizeof(record) /1024 /1024 << endl;
    ss << " readInputThroughput=" << sumReadInTuples /elapsed_time << endl;
    ss << " readBandWidth MB/s=" << (sumReadInTuples*sizeof(record)/1024/1024)/elapsed_time << endl;

    ss << " ----------------------------------------------" << endl;

    ss << " producedTuples=" << sumProducedTuples << endl;
    ss << " producedBuffers=" << sumProducedBuffer << endl;
    ss << " noFreeEntry=" << sumNoFreeEntry << endl;
    ss << " ProduceThroughput=" << sumProducedTuples / elapsed_time << endl;
    ss << " TransferVolume(MB)=" << sumProducedTuples*sizeof(Tuple)/1024/1024 << endl;
    ss << " TransferBandwidth MB/s=" << (sumProducedTuples*sizeof(Tuple)/1024/1024)/elapsed_time << endl;
    ss << " ----------------------------------------------" << endl;

    ss << " consumedTuples=" << sumConsumedTuples  << endl;
    ss << " consumedBuffers=" << sumConsumedBuffer  << endl;
    ss << " sumNoBufferFound=" << sumNoBuffer  << endl;
    cout << ss.str() << endl;

//    printHT(hashTable, campaingCnt);
}
//
//                    if(consumerID == 0 && rank == 1)
//                    {
//                        size_t expecedHTs = 2;
//                        size_t count = 0;
////                        cout << "process rest with rank=" << rank << " consumerID=" << consumerID << endl;
//
//                        while(count < expecedHTs)
//                        {
//                            for(size_t i = 0; i < expecedHTs; i++)
//                            {
//                               if(ht_sign_ready[i] == BUFFER_USED_FLAG)
//                               {
////                                   cout << " add received ht for id " << i << endl;
//                                   ht_sign_ready[i] = BUFFER_USED_SENDER_DONE;
//                                   std::atomic<size_t>* tempTable = (std::atomic<size_t>*) sharedHT_buffer[i]->getData();
//                                   for(size_t i = 0; i < campaingCnt; i++)
//                                   {
////                                       cout << "merge i=" << i << " old=" << outputTable[i] << " inc =" << tempTable[i] << endl;
//                                       outputTable[i] += tempTable[i];
//                                   }
//                                   count++;
//                               }
//                            }
//                        }
//                        for(size_t u = 0; u < expecedHTs; u++)
//                        {
////                            cout << "set buffer ready for consumerID=" << u << endl;
//                            ht_sign_ready[u] = BUFFER_READY_FLAG;
//
//                        }
////                        printSingleHT(outputTable, campaingCnt);
//                        std::fill(outputTable, outputTable + campaingCnt, 0);
//                    }
