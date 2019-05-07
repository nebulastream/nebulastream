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
#define PORT4 55358

//#define OLCONSUMERVERSION
//#define BUFFER_COUNT 10
#define BUFFER_USED_SENDER_DONE 127
#define BUFFER_READY_FLAG 0
#define BUFFER_USED_FLAG 1
#define BUFFER_BEING_PROCESSED_FLAG 2
#define BUFFER_WILL_BE_PROCESSED_FLAG 3

#define NUMBER_OF_GEN_TUPLE 1000000
//#define DEBUG
void printSingleHT(std::atomic<size_t>* hashTable, size_t campaingCnt);
//std::vector<std::shared_ptr<std::thread>> buffer_threads;
std::atomic<size_t>* outputTable;


std::atomic<size_t> exitProducer;
std::atomic<size_t> exitConsumer;

size_t NUM_SEND_BUFFERS;

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
//        buffer_ready_sign = other.buffer_ready_sign;
        region_tokens = other.region_tokens;
        sign_buffer = other.sign_buffer;
        sign_token = other.sign_token;
        sendBuffers = other.sendBuffers;
        records = other.records;
        con = other.con;
        bookKeeping = other.bookKeeping;
//        exitProducer = other.exitProducer;
//        exitConsumer = other.exitConsumer;
    }

    //consumer stuff
    infinity::memory::Buffer** recv_buffers;
    uint64_t* buffer_ready_sign;

    //both producer and consumer
    infinity::memory::RegionToken** region_tokens;

    //producer stuff
    infinity::memory::Buffer* sign_buffer;//reads the buffer_read from customer into this
    RegionToken* sign_token;//special token for this connection
    TupleBuffer** sendBuffers;
    record** records;
    VerbsConnection* con;
    std::atomic<size_t>** hashTable;
    tbb::atomic<size_t>* bookKeeping;

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

size_t produce_window_mem_two_buffers(record* records, size_t bufferSize, Tuple* outputBufferEven, Tuple* outputBufferOdd,
        size_t* tupCntBuffEven, size_t* tupCntBuffOdd)
{
    size_t bufferIndexEven = 0;
    size_t bufferIndexOdd = 0;
    size_t inputTupsIndex = 0;
    size_t readTuples = 0;

    while(bufferIndexEven < bufferSize && bufferIndexOdd < bufferSize )
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

        if(hashValue.value % 2 == 0)
        {
            outputBufferEven[bufferIndexEven].campaign_id = hashValue.value;
            outputBufferEven[bufferIndexEven].timeStamp = timeStamp;
            bufferIndexEven++;
        }
        else
        {
            outputBufferOdd[bufferIndexOdd].campaign_id = hashValue.value;
            outputBufferOdd[bufferIndexOdd].timeStamp = timeStamp;
            bufferIndexOdd++;
        }

        if(inputTupsIndex < NUMBER_OF_GEN_TUPLE)
            inputTupsIndex++;
        else
            inputTupsIndex = 0;
    }
    *tupCntBuffEven = bufferIndexEven;
    *tupCntBuffOdd = bufferIndexOdd;
    return readTuples;
}

void runProducerOneOnOneTwoNodes(VerbsConnection* connection, record* records, size_t bufferSizeInTuples, size_t bufferProcCnt,
        size_t* producesTuples, size_t* producedBuffers, size_t* readInputTuples, size_t* noFreeEntryFound, size_t startIdx,
        size_t endIdx, size_t numberOfProducer, ConnectionInfos* cInfos, size_t outerThread)
{
    assert(0);
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
                connection->read_blocking(cInfos->sign_buffer, cInfos->sign_token, startIdx*sizeof(size_t), startIdx *sizeof(size_t), (endIdx - startIdx)* sizeof(uint64_t));
            }
            if(cInfos->buffer_ready_sign[receive_buffer_index] == BUFFER_READY_FLAG)
            {
                size_t prevValue = connection->atomic_cas_blocking(cInfos->sign_token, receive_buffer_index*sizeof(size_t), BUFFER_READY_FLAG, BUFFER_BEING_PROCESSED_FLAG, nullptr);
                if(prevValue != BUFFER_READY_FLAG)
                {
                    cout << "buffer already taken with val=" << prevValue << endl;
                    continue;
                }
                else
                {
//                    cout << "buffer free" << endl;
                }

                //this will run until one buffer is filled completely
                readTuples += produce_window_mem(records, bufferSizeInTuples, cInfos->sendBuffers[receive_buffer_index]->tups);
                cInfos->sendBuffers[receive_buffer_index]->numberOfTuples = bufferSizeInTuples;

                connection->write(cInfos->sendBuffers[receive_buffer_index]->send_buffer, cInfos->region_tokens[receive_buffer_index],
                        cInfos->sendBuffers[receive_buffer_index]->requestToken);
#ifdef DEBUG
                cout << "Thread:" << outerThread << "/" << omp_get_thread_num()  << " Writing " << cInfos->sendBuffers[receive_buffer_index]->numberOfTuples << " tuples on buffer "
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
                    connection->write_blocking(cInfos->sign_buffer, cInfos->sign_token, receive_buffer_index*sizeof(size_t), receive_buffer_index*sizeof(size_t), sizeof(size_t));
                }
                else//finished processing
                {
                    std::atomic_fetch_add(&exitProducer, size_t(1));
                    if(std::atomic_load(&exitProducer) == numberOfProducer)
                    {
                        cInfos->buffer_ready_sign[receive_buffer_index] = BUFFER_USED_SENDER_DONE;
                        connection->write_blocking(cInfos->sign_buffer, cInfos->sign_token, receive_buffer_index*sizeof(size_t), receive_buffer_index*sizeof(size_t), sizeof(uint64_t));
                        cout << "Sent last tuples and marked as BUFFER_USED_SENDER_DONE at index=" << receive_buffer_index << " numanode=" << outerThread << endl;
                    }
                    else
                    {
                        cInfos->buffer_ready_sign[receive_buffer_index] = BUFFER_USED_FLAG;
                        connection->write(cInfos->sign_buffer, cInfos->sign_token, receive_buffer_index*sizeof(size_t), receive_buffer_index*sizeof(size_t), sizeof(uint64_t));
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
                 cout << "Thread:" << outerThread << "/" << omp_get_thread_num()
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


void runProducerOneOnOneFourNodes(record* records, size_t bufferSizeInTuples, size_t bufferProcCnt,
        size_t* producesTuples, size_t* producedBuffers, size_t* readInputTuples, size_t* noFreeEntryFound, size_t startIdx,
        size_t endIdx, size_t numberOfProducer, ConnectionInfos** cInfos, size_t outerThread, size_t numberOfNodes)
{
    size_t total_sent_tuples = 0;
    size_t total_buffer_send = 0;
//    size_t send_buffer_index = 0;
    size_t readTuples = 0;
    size_t noBufferFreeToSend = 0;

    size_t idxConEven = 0;
    size_t idxConOdd = 0;
    size_t bufferSendToC1 = 0;
    size_t bufferSendToC2 = 0;
    size_t tupleSendToC1 = 0;
    size_t tupleSendToC2 = 0;
    size_t offsetConnectionEven = outerThread == 0 ? 0 : 2;
    size_t offsetConnectionOdd = outerThread == 0 ? 1 : 3;

    while(total_buffer_send < bufferProcCnt)
    {
        //alloc first buffer
        cout << " read idx for even con " << offsetConnectionEven <<  endl;
        for(size_t receive_buffer_index = startIdx; receive_buffer_index < endIdx && total_buffer_send < bufferProcCnt; receive_buffer_index++)
        {
            if(receive_buffer_index == startIdx)//read buffers
            {
                cout << " read array at idx=" << receive_buffer_index <<  endl;
                cInfos[offsetConnectionEven]->con->read_blocking(cInfos[offsetConnectionEven]->sign_buffer, cInfos[offsetConnectionEven]->sign_token,
                        startIdx*sizeof(size_t), startIdx *sizeof(size_t), (endIdx - startIdx)* sizeof(uint64_t));
            }

            if(cInfos[offsetConnectionEven]->buffer_ready_sign[receive_buffer_index] == BUFFER_READY_FLAG)
            {
                size_t prevValue = cInfos[offsetConnectionEven]->con->atomic_cas_blocking(cInfos[offsetConnectionEven]->sign_token,
                        receive_buffer_index*sizeof(size_t), BUFFER_READY_FLAG, BUFFER_BEING_PROCESSED_FLAG, nullptr);
                if(prevValue != BUFFER_READY_FLAG)
                {
                    cout << "numanode=" << outerThread << " buffer already taken with val=" << prevValue << " connection=" << offsetConnectionEven << endl;
                    continue;
                }
                else
                {
                    cout << "numanode=" << outerThread << " found first idx=" << receive_buffer_index << " connection=" << offsetConnectionEven << endl;
                    idxConEven = receive_buffer_index;
                    break;
                }
            }
            else
            {
//                cout << "numanode=" << outerThread << " val at idx=" << receive_buffer_index<< " is " << cInfos[offsetConnectionEven]->buffer_ready_sign[receive_buffer_index] << endl;
            }

            if(receive_buffer_index +1 > endIdx)
            {
                receive_buffer_index = startIdx;
            }
        }//end of for
        //alloc second buffer
        for(size_t receive_buffer_index = startIdx; receive_buffer_index < endIdx && total_buffer_send < bufferProcCnt; receive_buffer_index++)
        {
            if(receive_buffer_index == startIdx)//read buffers
            {
                cInfos[offsetConnectionOdd]->con->read_blocking(cInfos[offsetConnectionOdd]->sign_buffer, cInfos[offsetConnectionOdd]->sign_token,
                        startIdx*sizeof(size_t), startIdx *sizeof(size_t), (endIdx - startIdx)* sizeof(uint64_t));
            }
            if(cInfos[offsetConnectionOdd]->buffer_ready_sign[receive_buffer_index] == BUFFER_READY_FLAG)
            {
                size_t prevValue = cInfos[offsetConnectionOdd]->con->atomic_cas_blocking(cInfos[offsetConnectionOdd]->sign_token,
                        receive_buffer_index*sizeof(size_t), BUFFER_READY_FLAG, BUFFER_BEING_PROCESSED_FLAG, nullptr);
                if(prevValue != BUFFER_READY_FLAG)
                {
                    cout << "numanode=" << outerThread << " buffer already taken with val=" << prevValue  << " connection=" << offsetConnectionOdd << endl;
                    continue;
                }
                else
                {
                    cout << "numanode=" << outerThread << " found second idx=" << receive_buffer_index  << " connection=" << offsetConnectionOdd << endl;
                    idxConOdd = receive_buffer_index;
                    break;
                }
            }
            if(receive_buffer_index +1 > endIdx)
            {
                receive_buffer_index = startIdx;
            }
        }//end of for
        size_t* tupCntBuffEven = new size_t(0);
        size_t* tupCntBuffOdd = new size_t(0);
        readTuples += produce_window_mem_two_buffers(records, bufferSizeInTuples, cInfos[offsetConnectionEven]->sendBuffers[idxConEven]->tups, cInfos[offsetConnectionOdd]->sendBuffers[idxConOdd]->tups,
                tupCntBuffEven, tupCntBuffOdd);

        cout << "numanode=" << outerThread << " buffer fill finsihed bufferEven=" << *tupCntBuffEven<< " bufferOdd=" << *tupCntBuffOdd << endl;

        cInfos[offsetConnectionEven]->sendBuffers[idxConEven]->numberOfTuples = *tupCntBuffEven;
        cInfos[offsetConnectionOdd]->sendBuffers[idxConOdd]->numberOfTuples = *tupCntBuffOdd;

        total_sent_tuples += *tupCntBuffEven;
        tupleSendToC1 += *tupCntBuffEven;
        bufferSendToC1++;
        total_sent_tuples += *tupCntBuffOdd;
        tupleSendToC2 += *tupCntBuffOdd;
        bufferSendToC2++;

        cInfos[offsetConnectionEven]->con->write(cInfos[offsetConnectionEven]->sendBuffers[idxConEven]->send_buffer, cInfos[offsetConnectionEven]->region_tokens[idxConEven],
                cInfos[offsetConnectionEven]->sendBuffers[idxConEven]->requestToken);

        cInfos[offsetConnectionOdd]->con->write(cInfos[offsetConnectionOdd]->sendBuffers[idxConOdd]->send_buffer,
                cInfos[offsetConnectionOdd]->region_tokens[idxConOdd],
                cInfos[offsetConnectionOdd]->sendBuffers[idxConOdd]->requestToken);

        cout << "numanode=" << outerThread << " buffer sending finsihed" << endl;
        total_buffer_send += 2;

        if (total_buffer_send < bufferProcCnt)//a new buffer will be send next
        {
            cInfos[offsetConnectionEven]->buffer_ready_sign[idxConEven] = BUFFER_USED_FLAG;
            cInfos[offsetConnectionOdd]->buffer_ready_sign[idxConOdd] = BUFFER_USED_FLAG;

            cInfos[offsetConnectionEven]->con->write_blocking(cInfos[offsetConnectionEven]->sign_buffer, cInfos[offsetConnectionEven]->sign_token,
                    idxConEven*sizeof(size_t), idxConEven*sizeof(size_t), sizeof(size_t));
            cInfos[offsetConnectionOdd]->con->write_blocking(cInfos[offsetConnectionOdd]->sign_buffer, cInfos[offsetConnectionOdd]->sign_token, idxConOdd*sizeof(size_t),
                    idxConOdd*sizeof(size_t), sizeof(size_t));
            cout << "numanode=" << outerThread << " reset buffer" << endl;
        }
        else//finished processing
        {
            std::atomic_fetch_add(&exitProducer, size_t(1));//TODO: IS THIS OK?
            if(std::atomic_load(&exitProducer) == numberOfProducer)
            {
                cInfos[offsetConnectionEven]->buffer_ready_sign[idxConEven] = BUFFER_USED_SENDER_DONE;
                cInfos[offsetConnectionOdd]->buffer_ready_sign[idxConOdd] = BUFFER_USED_SENDER_DONE;

                cInfos[offsetConnectionEven]->con->write_blocking(cInfos[offsetConnectionEven]->sign_buffer,
                        cInfos[offsetConnectionEven]->sign_token, idxConEven*sizeof(size_t), idxConEven*sizeof(size_t), sizeof(uint64_t));
                cout << "numanode=" << outerThread << " Sent last tuples and marked as BUFFER_USED_SENDER_DONE at index=" << idxConEven << " numanode=" << outerThread << " con=" << offsetConnectionEven << endl;

                cInfos[offsetConnectionOdd]->con->write_blocking(cInfos[offsetConnectionOdd]->sign_buffer, cInfos[offsetConnectionOdd]->sign_token,
                        idxConOdd*sizeof(size_t), idxConOdd*sizeof(size_t), sizeof(uint64_t));
                cout << "numanode=" << outerThread << " Sent last tuples and marked as BUFFER_USED_SENDER_DONE at index=" << idxConOdd << " numanode=" << outerThread << " con=" << offsetConnectionOdd << endl;
            }
            else
            {
                cInfos[offsetConnectionEven]->buffer_ready_sign[idxConEven] = BUFFER_USED_FLAG;
                cInfos[offsetConnectionEven]->con->write(cInfos[offsetConnectionEven]->sign_buffer,
                        cInfos[offsetConnectionEven]->sign_token, idxConEven*sizeof(size_t), idxConEven*sizeof(size_t), sizeof(uint64_t));

                cInfos[offsetConnectionOdd]->buffer_ready_sign[idxConOdd] = BUFFER_USED_FLAG;
                cInfos[offsetConnectionOdd]->con->write(cInfos[offsetConnectionOdd]->sign_buffer,
                        cInfos[offsetConnectionOdd]->sign_token, idxConOdd*sizeof(size_t), idxConOdd*sizeof(size_t), sizeof(uint64_t));

                cout << "numanode=" << outerThread << " prod finished" << endl;
            }
            break;
        }
    }//end of while
//    cout << "Thread=" << omp_get_thread_num() << " Done sending! Sent a total of " << total_sent_tuples << " tuples and " << total_buffer_send << " buffers"
//            << " noBufferFreeToSend=" << noBufferFreeToSend << " startIDX=" << startIdx << " endIDX=" << endIdx << endl;
//#ifdef DEBUG

    #pragma omp critical
             {
                 cout << "Thread:" << outerThread << "/" << omp_get_thread_num()
                         << " producesTuples=" << total_sent_tuples
                         << " producedBuffers=" << total_buffer_send
                         << " bufferSendToC1(Even)=" << bufferSendToC1
                         << " bufferSendToC2(Odd)=" << bufferSendToC2
                         << " tupleSendToC1(Even)=" << tupleSendToC1
                         << " tupleSendToC2(Odd)=" << tupleSendToC2
                         << " readInputTuples=" << readTuples
                         << " noFreeEntryFound=" << noBufferFreeToSend
                         << endl;

             }
//#endif
    *producesTuples = total_sent_tuples;
    *producedBuffers = total_buffer_send;
    *readInputTuples = readTuples;
    *noFreeEntryFound = noBufferFreeToSend;
}

size_t runConsumerOneOnOne(Tuple* buffer, size_t bufferSizeInTuples, std::atomic<size_t>** hashTable, size_t windowSizeInSec,
        size_t campaingCnt, size_t consumerID, size_t rank, bool done, tbb::atomic<size_t>* bookKeeper, std::atomic<size_t>* exitConsumer) {
    size_t consumed = 0;
    size_t windowSwitchCnt = 0;
    size_t htReset = 0;
    Tuple tup;
    size_t current_window = bookKeeper[0] > bookKeeper[1] ? 0 : 1;
    size_t lastTimeStamp = bookKeeper[current_window];

    for(size_t i = 0; i < bufferSizeInTuples; i++)
    {
        size_t timeStamp = time(NULL);//        = buffer[i].timeStamp; //was
        if (lastTimeStamp != timeStamp && timeStamp % windowSizeInSec == 0)
        {
            if(bookKeeper[current_window].compare_and_swap(timeStamp, lastTimeStamp) == lastTimeStamp)
            {
                    htReset++;
//                    #pragma omp critical
//                    {
//                        cout << "windowing with rank=" << rank << " consumerID=" << consumerID << "ts=" << timeStamp
//                            << " lastts=" << lastTimeStamp << " thread=" << omp_get_thread_num()
//                            << " i=" << i  << " done=" << done << " exit=" << *exitConsumer << endl;
//                    }
                    if(rank == 3 && !done && std::atomic_load(exitConsumer) != 1)
                    {
//                        memcpy(sharedHT_buffer[consumerID]->getData(), hashTable[current_window], sizeof(std::atomic<size_t>) * campaingCnt);
//
////                        cout << "send blocking id=" << consumerID  << endl;
//                        sharedHTConnection->send(sharedHT_buffer[consumerID]);//send_blocking
//                        cout << "send blocking finished " << endl;
                    }
                    else if(rank == 1 && !done && std::atomic_load(exitConsumer) != 1)//this one merges
                    {
//                        cout << "merging local stuff for consumerID=" << consumerID << endl;
                        #pragma omp parallel for num_threads(20)
                        for(size_t i = 0; i < campaingCnt; i++)
                        {
                            outputTable[i] += hashTable[current_window][i];
                        }
//                        cout << "post rec id " << consumerID << " ranK=" << rank << " thread=" << omp_get_thread_num() << "done=" << done<< endl;
                        //TODO: DO THIS AS LAMDA FUNC CALL

//                        buffer_threads.push_back(std::make_shared<std::thread>([&sharedHTConnection, consumerID, exitConsumer,
//                          sharedHT_buffer, outputTable, campaingCnt] {
//                            cout << "run buffer thread" << endl;
//                            ReceiveElement receiveElement;
//                            receiveElement.buffer = sharedHT_buffer[consumerID];
//                            sharedHTConnection->post_receive(receiveElement.buffer);
//                            while(!sharedHTConnection->check_receive(receiveElement) && std::atomic_load(exitConsumer) != 1)
//                            {
//    //                            cout << "wait receive " << std::atomic_load(exitConsumer)<< endl;
//    //                            sleep(1);
//                            }
//                            cout << "revceived" << endl;
//    //                        sharedHTConnection->post_and_receive_blocking(sharedHT_buffer[consumerID]);
//
//    //                        cout << "got rec" << endl;
//                            std::atomic<size_t>* tempTable = (std::atomic<size_t>*) sharedHT_buffer[consumerID]->getData();
//
//                            #pragma omp parallel for num_threads(20)
//                            for(size_t i = 0; i < campaingCnt; i++)
//                            {
//                                outputTable[i] += tempTable[i];
//                            }
//
//
//                        }));
#if 0
                        ReceiveElement receiveElement;
                        receiveElement.buffer = sharedHT_buffer[consumerID];
                        sharedHTConnection->post_receive(receiveElement.buffer);
                        while(!sharedHTConnection->check_receive(receiveElement) && std::atomic_load(exitConsumer) != 1)
                        {
//                            cout << "wait receive " << std::atomic_load(exitConsumer)<< endl;
//                            sleep(1);
                        }
                        cout << "revceived" << endl;
//                        sharedHTConnection->post_and_receive_blocking(sharedHT_buffer[consumerID]);

//                        cout << "got rec" << endl;
                        std::atomic<size_t>* tempTable = (std::atomic<size_t>*) sharedHT_buffer[consumerID]->getData();

                        #pragma omp parallel for num_threads(20)
                        for(size_t i = 0; i < campaingCnt; i++)
                        {
                            outputTable[i] += tempTable[i];
                        }
#endif
                    }//end of else
            }//end of if to change
            current_window = current_window == 0 ? 1 : 0;
            lastTimeStamp = timeStamp;
            windowSwitchCnt++;

        }//end of if window is new
//        }//end of for
        uint64_t bucketPos = (buffer[i].campaign_id * 789 + 321) % campaingCnt;
        atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
        consumed++;

    }       //end of for
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
        size_t startIdx, size_t endIdx, ConnectionInfos* cInfos, size_t outerThread, size_t rank, size_t numberOfNodes)
{
    size_t total_received_tuples = 0;
    size_t total_received_buffers = 0;
    size_t index = startIdx;
    size_t noBufferFound = 0;
    size_t consumed = 0;
    bool is_done = numberOfNodes == 4 ? false : true;
    while(true)
    {
        if (cInfos->buffer_ready_sign[index] == BUFFER_USED_FLAG || cInfos->buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE)
        {
            if(cInfos->buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE)
            {
                    std::atomic_fetch_add(&exitConsumer, size_t(1));
                    cout << "DONE BUFFER FOUND at idx"  << index << " numanode=" << outerThread << endl;
                    is_done = true;
            }
            else
            {
                cout << " found buffer at idx=" << index << endl;
            }

            total_received_tuples += bufferSizeInTuples;
            total_received_buffers++;
#ifdef DEBUG
            cout << "Thread=" << outerThread << "/" << omp_get_thread_num() << "/" << outerThread<< " Received buffer at index=" << index << endl;
#endif
            consumed += runConsumerOneOnOne((Tuple*)cInfos->recv_buffers[index]->getData(), bufferSizeInTuples,
                    hashTable, windowSizeInSec, campaingCnt, consumerID, rank, is_done, cInfos->bookKeeping, &exitConsumer);

            cInfos->buffer_ready_sign[index] = BUFFER_READY_FLAG;
            cout << " set buffer ready again" << endl;
        }
        else
        {
            noBufferFound++;
        }

        index++;

        if(index > endIdx)
            index = startIdx;

        if(exitConsumer == 1)
        {
//            *consumedTuples = total_received_tuples;
//            *consumedBuffers = total_received_buffers;
#ifdef DEBUG

            stringstream ss;
            cout << "before out Thread=" << outerThread << "/" << omp_get_thread_num() << " Receiving a total of " << total_received_tuples << " tuples and " << total_received_buffers << " buffers"
                            << " nobufferFound=" << noBufferFound << " startIDX=" << startIdx << " endIDX=" << endIdx << endl;
            cout << ss.str();
#endif
            break;
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
                                hashTable, windowSizeInSec, campaingCnt, consumerID, rank, /*is_done*/ true, cInfos->bookKeeping, &exitConsumer);
            cInfos->buffer_ready_sign[index] = BUFFER_READY_FLAG;
        }
    }

    *consumedTuples = consumed;
    *consumedBuffers = total_received_buffers;
    *consumerNoBufferFound = noBufferFound;
    cout << "Thread=" << omp_get_thread_num() << " Done Receiving a total of " << total_received_tuples << " tuples and " << total_received_buffers << " buffers"
                << " nobufferFound=" << noBufferFound << " startIDX=" << startIdx << " endIDX=" << endIdx << endl;
}


ConnectionInfos* setupRDMAConsumer(VerbsConnection* connection, size_t bufferSizeInTups, size_t campaingCnt)
{
    ConnectionInfos* connectInfo = new ConnectionInfos();

    auto outer_thread_id = omp_get_thread_num();
    numa_run_on_node(outer_thread_id);
    numa_set_preferred(outer_thread_id);

    void* b1 = numa_alloc_onnode(NUM_SEND_BUFFERS*sizeof(size_t), outer_thread_id);
    connectInfo->buffer_ready_sign = (size_t*)b1;
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

    stringstream s2;
    for(size_t i = 0; i <= NUM_SEND_BUFFERS; i++)
    {
        if (i < NUM_SEND_BUFFERS) {
            void* b1 = numa_alloc_onnode(bufferSizeInTups * sizeof(Tuple), outer_thread_id);
//            connectInfo->buffer_ready_sign = (size_t*)b1
//            connectInfo->recv_buffers[i] = connection->allocate_buffer(bufferSizeInTups * sizeof(Tuple));
            connectInfo->recv_buffers[i] = connection->register_buffer(b1, bufferSizeInTups * sizeof(Tuple));
            connectInfo->region_tokens[i] = connectInfo->recv_buffers[i]->createRegionToken();

//            if(outer_thread_id == 0)
                s2 << "i=" << i << "recv region getSizeInBytes=" << connectInfo->recv_buffers[i]->getSizeInBytes() << " getAddress=" << connectInfo->recv_buffers[i]->getAddress()
                                       << " getLocalKey=" << connectInfo->recv_buffers[i]->getLocalKey() << " getRemoteKey=" << connectInfo->recv_buffers[i]->getRemoteKey() << endl;
        } else {
//            cout << "copy sign token at pos " << i << endl;
            connectInfo->sign_buffer = connection->register_buffer(connectInfo->buffer_ready_sign, NUM_SEND_BUFFERS*sizeof(size_t));
//            connectInfo->sign_buffer = connection->allocate_buffer(NUM_SEND_BUFFERS*sizeof(infinity::memory::Atomic));
            connectInfo->region_tokens[i] = connectInfo->sign_buffer->createRegionToken();

//            if(outer_thread_id == 0)
                s2 << "i=" << i << "sign region getSizeInBytes=" << connectInfo->sign_buffer->getSizeInBytes() << " getAddress=" << connectInfo->sign_buffer->getAddress()
                                       << " getLocalKey=" << connectInfo->sign_buffer->getLocalKey() << " getRemoteKey=" << connectInfo->sign_buffer->getRemoteKey() << endl;

        }
        memcpy((RegionToken*)tokenbuffer->getData() + i, connectInfo->region_tokens[i], sizeof(RegionToken));
    }
    cout << s2.str() << endl;
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

    connectInfo->bookKeeping = new tbb::atomic<size_t>[2];
    connectInfo->bookKeeping[0] = time(NULL);
    connectInfo->bookKeeping[1] = time(NULL) + 2;

    exitConsumer = 0;

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
        connectInfo->sendBuffers[i] = new TupleBuffer(*connection, bufferSizeInTups);//TODO:not sure if this is right
    }

    void* b3 = numa_alloc_onnode(NUM_SEND_BUFFERS*sizeof(size_t), outer_thread_id);
    connectInfo->buffer_ready_sign = (size_t*)b3;
    for(size_t i = 0; i < NUM_SEND_BUFFERS; i++)
    {
      connectInfo->buffer_ready_sign[i] = BUFFER_READY_FLAG;
    }

    connectInfo->sign_buffer = connection->register_buffer(connectInfo->buffer_ready_sign, NUM_SEND_BUFFERS*sizeof(size_t));

//    cout << "prod sign buffer size=" << connectInfo->sign_buffer->getSizeInBytes() << endl;
    infinity::memory::Buffer* tokenbuffer = connection->allocate_buffer((NUM_SEND_BUFFERS+1) * sizeof(RegionToken));

    void* b2 = numa_alloc_onnode((NUM_SEND_BUFFERS+1)*sizeof(RegionToken), outer_thread_id);
    connectInfo->region_tokens = (infinity::memory::RegionToken**)b2;

    connection->post_and_receive_blocking(tokenbuffer);
    cout << "got receive " << endl;
    stringstream s2;
    for(size_t i = 0; i <= NUM_SEND_BUFFERS; i++)
    {
        if ( i < NUM_SEND_BUFFERS){
            connectInfo->region_tokens[i] = static_cast<RegionToken*>(numa_alloc_onnode(sizeof(RegionToken), outer_thread_id));
            memcpy(connectInfo->region_tokens[i], (RegionToken*)tokenbuffer->getData() + i, sizeof(RegionToken));
                s2 << "region getSizeInBytes=" << connectInfo->region_tokens[i]->getSizeInBytes() << " getAddress=" << connectInfo->region_tokens[i]->getAddress()
                    << " getLocalKey=" << connectInfo->region_tokens[i]->getLocalKey() << " getRemoteKey=" << connectInfo->region_tokens[i]->getRemoteKey() << endl;
        }
        else {
            connectInfo->sign_token = static_cast<RegionToken*>(numa_alloc_onnode(sizeof(RegionToken), outer_thread_id));
            memcpy(connectInfo->sign_token, (RegionToken*)tokenbuffer->getData() + i, sizeof(RegionToken));

                s2 << " SIGN LOCALregion getSizeInBytes=" << connectInfo->sign_token->getSizeInBytes() << " getAddress=" << connectInfo->sign_token->getAddress()
                    << " getLocalKey=" << connectInfo->sign_token->getLocalKey() << " getRemoteKey=" << connectInfo->sign_token->getRemoteKey() << endl;
        }
    }

   cout << s2.str() << endl;

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

   exitProducer = 0;

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
        randomNumbers[i] = (size_t)disi(gen);

//    record** recs;
    uint64_t campaign_lsb, campaign_msb;
    uint64_t uuid = diss(gen);
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

size_t getNumaNodeFromPtr(void* ptr, std::string name)
{
    int numa_node1 = -1;
    get_mempolicy(&numa_node1, NULL, 0, ptr, MPOL_F_NODE | MPOL_F_ADDR);

    int status[1];
    status[0]=-1;
    int ret_code = move_pages(0 /*selbuffer_threadsf memory */, 1, &ptr, NULL, status, 0);
    int numa_node2 = status[0];
    if(numa_node1 != numa_node2)
        cout << "Warning no matching numa node for " << name << endl;

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
//    size_t rank = atoi(argv[1]);

    size_t numberOfProducer = 2;
    size_t numberOfConsumer = 2;
    size_t bufferProcCnt = 10;
    size_t bufferSizeInTups = 10;
    size_t numberOfConnections = 1;
    NUM_SEND_BUFFERS = 10;
    size_t num_send_buffer = 10;

    size_t numaNodes = 2;
    size_t numberOfNodes = 4;
    string ip = "192.168.5.10";

    desc.add_options()
        ("help", "Print help messages")
        ("rank", po::value<size_t>(&rank)->default_value(rank), "The rank of the current runtime")
        ("numberOfProducer", po::value<size_t>(&numberOfProducer)->default_value(numberOfProducer), "numberOfProducer")
        ("numberOfConsumer", po::value<size_t>(&numberOfConsumer)->default_value(numberOfConsumer), "numberOfConsumer")
        ("bufferProcCnt", po::value<size_t>(&bufferProcCnt)->default_value(bufferProcCnt), "bufferProcCnt")
        ("bufferSizeInTups", po::value<size_t>(&bufferSizeInTups)->default_value(bufferSizeInTups), "bufferSizeInTups")
        ("sendBuffers", po::value<size_t>(&num_send_buffer)->default_value(num_send_buffer), "sendBuffers")
        ("numberOfConnections", po::value<size_t>(&numberOfConnections)->default_value(numberOfConnections), "numberOfConnections")
        ("numberOfNodes", po::value<size_t>(&numberOfNodes)->default_value(numberOfNodes), "numberOfConnections")
        ("numaNodes", po::value<size_t>(&numaNodes)->default_value(numaNodes), "numaNodes")
        ("ip", po::value<string>(&ip)->default_value(ip), "ip");
//        ;
//
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
            << " Rank=" << rank
            << " genCnt=" << NUMBER_OF_GEN_TUPLE
            << " bufferSizeInTups=" << bufferSizeInTups
            << " bufferSizeInKB=" << bufferSizeInTups * sizeof(Tuple) / 1024
            << " numberOfSendBuffer=" << NUM_SEND_BUFFERS
            << " numberOfProducer=" << numberOfProducer
            << " numberOfConsumer=" << numberOfConsumer
            << " numberOfConnections=" << numberOfConnections
            << " numberOfNodes=" << numberOfNodes
            << " numaNodes=" << numaNodes
            << " ip=" << ip
            << endl;



//    htPtrs = new std::atomic<size_t>*[4];
    ConnectionInfos** conInfos;
    if(numberOfNodes == 2)
        conInfos = new ConnectionInfos*[numaNodes];
    else
        conInfos = new ConnectionInfos*[numaNodes*2];// numanode_0: 0,2 numanod_1: 1,3

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
        target_rank = 0;
    else
        assert(0);

    omp_set_nested(1);

    if(rank == 0)
    {
        cout << "starting " << numaNodes << " threads to connect node 1" << endl;
        #pragma omp parallel num_threads(numaNodes)
        {
            #pragma omp critical
            {
                cout << "thread in critical = " << omp_get_thread_num() << endl;
                if(numberOfConnections == 1)
                {
                    size_t numaNode = omp_get_thread_num();
                    size_t connectionID = omp_get_thread_num() * 2;
                    cout << "rank 0: connecting 0 and 1 on numa node " << numaNode << " connectionID=" << connectionID << endl;

                    if(numaNode == 0)
                    {
                        SimpleInfoProvider info(target_rank, "mlx5_0", 1, PORT1, ip);//was 3
                        connections[connectionID] = new VerbsConnection(&info);
                        cout << "connection established rank 0 and 1 on numa node " << numaNode  << " connectionID=" << connectionID << endl;
                    }
                    else
                    {
                        SimpleInfoProvider info(target_rank, "mlx5_1", 1, PORT2, ip);//was 3
                        connections[connectionID] = new VerbsConnection(&info);
                        cout << "connection established rank 0 and 1 on numa node " << numaNode  << " connectionID=" << connectionID << endl;
                    }

                    cout << "setup con numa node " << numaNode  << " connectionID=" << connectionID << endl;
                    conInfos[connectionID] = setupRDMAProducer(connections[connectionID], bufferSizeInTups);
                    conInfos[connectionID]->con = connections[connectionID];
                    cout << "setup con numa node " << numaNode  << " connectionID=" << connectionID << " finished"<< endl;
                }
                else
                    assert(0);

                conInfos[omp_get_thread_num()]->records = new record*[numberOfProducer];
                for(size_t i = 0; i < numberOfProducer; i++)
                {
                    conInfos[omp_get_thread_num()]->records[i] = generateTuplesOneArray(numberOfProducer, campaingCnt);
                }
                int numa_node = -1;
                get_mempolicy(&numa_node, NULL, 0, (void*)conInfos[omp_get_thread_num()]->records[0], MPOL_F_NODE | MPOL_F_ADDR);
                cout << "ht numa=" << numa_node << " outthread=" << omp_get_thread_num() << endl;
            }
            cout << "thread out of critical = " << omp_get_thread_num() << endl;
        }//end of pragma

        cout << "starting " << numaNodes << " threads to connect node 2" << endl;
        #pragma omp parallel for num_threads(numaNodes)
        for(size_t i = 0; i < numaNodes; i++)
        {
//            #pragma omp critical
//            {
                target_rank = 3;
                cout << "thread in critical = " << omp_get_thread_num() << endl;
                if(numberOfConnections == 1)
                {
                    size_t numaNode = omp_get_thread_num();
                    size_t connectionID = (omp_get_thread_num() * 2) +1;
                    cout << "rank 0: connecting 0 and 3 on numa node " << numaNode << " connectionID=" << connectionID << endl;

                    if(numaNode == 0)
                    {
                        cout << " waiting on port " << PORT3 << " numaNode=" << numaNode << endl;
                        SimpleInfoProvider info(target_rank, "mlx5_2", 1, PORT3, ip);//was 3
                        connections[connectionID] = new VerbsConnection(&info);
                        cout << "connection established rank 0 and 3 on numa node " << numaNode  << " connectionID=" << connectionID << endl;
                    }
                    else
                    {
                        cout << " waiting on port " << PORT4 << " numaNode=" << numaNode << endl;
                        SimpleInfoProvider info(target_rank, "mlx5_3", 1, PORT4, ip);//was 3
                        connections[connectionID] = new VerbsConnection(&info);
                        cout << "connection established rank 0 and 3 on numa node " << numaNode  << " connectionID=" << connectionID << endl;
                    }

                    cout << "setup con numa node " << numaNode  << " connectionID=" << connectionID << endl;
                    conInfos[connectionID] = setupRDMAProducer(connections[connectionID], bufferSizeInTups);
                    conInfos[connectionID]->con = connections[connectionID];
                    cout << "setup con numa node " << numaNode  << " connectionID=" << connectionID << " finished"<< endl;
                }
                else
                    assert(0);

                conInfos[omp_get_thread_num()]->records = new record*[numberOfProducer];//TODO: is this still neccesary?
                for(size_t i = 0; i < numberOfProducer; i++)
                {
                    conInfos[omp_get_thread_num()]->records[i] = generateTuplesOneArray(numberOfProducer, campaingCnt);
                }
                int numa_node = -1;
                get_mempolicy(&numa_node, NULL, 0, (void*)conInfos[omp_get_thread_num()]->records[0], MPOL_F_NODE | MPOL_F_ADDR);
                cout << "ht numa=" << numa_node << " outthread=" << omp_get_thread_num() << endl;
            }
            cout << "thread out of critical = " << omp_get_thread_num() << endl;
//        }//end of pragma

    }
    if(rank == 1)
    {
//        #pragma omp parallel num_threads(numaNodes)
        #pragma omp parallel for num_threads(numaNodes)
        for(size_t i = 0; i < numaNodes; i++)
        {
            #pragma omp critical
            {
                cout << "thread in critical = " << omp_get_thread_num() << endl;
                if(numberOfConnections == 1)
                {
                    size_t numaNode = omp_get_thread_num();
//                    size_t connectionID = omp_get_thread_num() * 2;
                    if(numaNode == 0)
                    {
                       SimpleInfoProvider info(target_rank, "mlx5_0", 1, PORT1, ip);//was 3
                       connections[numaNode] = new VerbsConnection(&info);
                       cout << "connection established rank 0 and 1 on numa node " << numaNode  << endl;
                   }
                   else
                   {
                       SimpleInfoProvider info(target_rank, "mlx5_1", 1, PORT2, ip);//was 3
                       connections[numaNode] = new VerbsConnection(&info);
                       cout << "connection established rank 0 and 1 on numa node " << numaNode << endl;
                   }

                    conInfos[numaNode] = setupRDMAConsumer(connections[numaNode], bufferSizeInTups, campaingCnt);
                    conInfos[numaNode]->con = connections[numaNode];
                }
                else
                    assert(0);
            }//end of critical
            cout << "thread out of critical = " << omp_get_thread_num() << endl;
        }
    }
    if(rank == 3)
    {
        #pragma omp parallel for num_threads(numaNodes)
        for(size_t i = 0; i < numaNodes; i++)        {
            #pragma omp critical
            {
                cout << "thread in critical = " << omp_get_thread_num() << endl;
                if(numberOfConnections == 1)
                {
                    size_t numaNode = omp_get_thread_num();
//                    size_t connectionID = omp_get_thread_num() * 2;
                    if(numaNode == 0)
                    {
                       cout << " connection on port " << PORT3 << " numaNode=" << numaNode << endl;
                       SimpleInfoProvider info(target_rank, "mlx5_2", 1, PORT3, ip);//was 3
                       connections[numaNode] = new VerbsConnection(&info);
                       cout << "connection established rank 0 and 3 on numa node " << numaNode  << endl;
                   }
                   else
                   {
                       cout << " connection on port " << PORT4 << " numaNode=" << numaNode << endl;
                       SimpleInfoProvider info(target_rank, "mlx5_3", 1, PORT4, ip);//was 3
                       connections[numaNode] = new VerbsConnection(&info);
                       cout << "connection established rank 0 and 3 on numa node " << numaNode << endl;
                   }

                    conInfos[numaNode] = setupRDMAConsumer(connections[numaNode], bufferSizeInTups, campaingCnt);
                    conInfos[numaNode]->con = connections[numaNode];
                }
                else
                    assert(0);
            }//end of critical
            cout << "thread out of critical = " << omp_get_thread_num() << endl;
        }
    }

    size_t producesTuples[numaNodes][numberOfProducer/numaNodes] = {0};
    size_t producedBuffers[numaNodes][numberOfProducer/numaNodes] = {0};
    size_t noFreeEntryFound[numaNodes][numberOfProducer/numaNodes] = {0};
    size_t consumedTuples[numaNodes][numberOfConsumer/numaNodes] = {0};
    size_t consumedBuffers[numaNodes][numberOfConsumer/numaNodes] = {0};
    size_t consumerNoBufferFound[numaNodes][numberOfConsumer/numaNodes] = {0};
    size_t readInputTuples[numaNodes][numberOfProducer/numaNodes] = {0};

    infinity::memory::Buffer* finishBuffer = connections[0]->allocate_buffer(1);
    cout << "start processing " << endl;
    Timestamp begin = getTimestamp();
    if(rank % 2 == 0)
    {
    #pragma omp parallel num_threads(numaNodes)//nodes
       {
          auto outer_thread_id = omp_get_thread_num();
          numa_run_on_node(outer_thread_id);
          #pragma omp parallel num_threads(numberOfProducer/numaNodes)//
          {
             auto inner_thread_id = omp_get_thread_num();
             size_t i = inner_thread_id;
             size_t share = NUM_SEND_BUFFERS/(numberOfProducer/numaNodes);
             size_t startIdx = inner_thread_id* share;
             size_t endIdx = (inner_thread_id+1)*share;
             record* recs = conInfos[outer_thread_id]->records[inner_thread_id];

//#ifdef DEBUG
             #pragma omp critical
             {
                 std::cout
                << "OuterThread=" << outer_thread_id
                << " InnerThread=" << inner_thread_id
                << " SumThreadID=" << i
                << " core: " << sched_getcpu()
                << " numaNode:" << numa_node_of_cpu(sched_getcpu())
                << " sendBufferLocation=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->sendBuffers, "sendBufferLocation")
                << " sendBufferDataLocation=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->sendBuffers[0]->send_buffer, "sendBufferDataLocation")
                << " inputdata numaNode=" << getNumaNodeFromPtr(recs, "inputdata numaNode")
                << " start=" << startIdx
                << " endidx=" << endIdx
                << " share=" << share
                << std::endl;
             }
//#endif

             if(numberOfNodes == 2)
             {
                 runProducerOneOnOneTwoNodes(connections[0], recs, bufferSizeInTups, bufferProcCnt/numberOfProducer, &producesTuples[outer_thread_id][i],
                          &producedBuffers[outer_thread_id][i], &readInputTuples[outer_thread_id][i], &noFreeEntryFound[outer_thread_id][i], startIdx, endIdx,
                          numberOfProducer, conInfos[outer_thread_id], outer_thread_id);
             }
             else
             {

                 runProducerOneOnOneFourNodes(recs, bufferSizeInTups, bufferProcCnt/numberOfProducer, &producesTuples[outer_thread_id][i],
                         &producedBuffers[outer_thread_id][i], &readInputTuples[outer_thread_id][i], &noFreeEntryFound[outer_thread_id][i], startIdx, endIdx,
                         numberOfProducer, conInfos, outer_thread_id, numberOfNodes);
             }


             assert(outer_thread_id == numa_node_of_cpu(sched_getcpu()));
          }
       }
       cout << "producer finished ... waiting for consumer to finish " << getTimestamp() << endl;
       connections[0]->post_and_receive_blocking(finishBuffer);
//       connections[2]->post_and_receive_blocking(finishBuffer);
       cout << "got finish buffer, finished execution " << getTimestamp()<< endl;
    }
    else
    {
#pragma omp parallel num_threads(numaNodes)
       {
          auto outer_thread_id = omp_get_thread_num();
          numa_run_on_node(outer_thread_id);
          #pragma omp parallel num_threads(numberOfConsumer/numaNodes)
          {
             auto inner_thread_id = omp_get_thread_num();
             size_t i = inner_thread_id;
             size_t share = NUM_SEND_BUFFERS/(numberOfConsumer/numaNodes);
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
                << " receiveBufferLocation=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->recv_buffers, "receiveBufferLocation")
                << " receiveBufferDataLocation=" << getNumaNodeFromPtr(conInfos[outer_thread_id]->recv_buffers[0]->getData(), "receiveBufferDataLocation")
                << " start=" << startIdx
                << " endidx=" << endIdx
                << " share=" << share
                << std::endl;
             }
//#endif
             runConsumerNew(conInfos[outer_thread_id]->hashTable, windowSizeInSeconds, campaingCnt, outer_thread_id, numberOfProducer , bufferSizeInTups,
                     &consumedTuples[outer_thread_id][i], &consumedBuffers[outer_thread_id][i], &consumerNoBufferFound[outer_thread_id][i], startIdx,
                     endIdx, conInfos[outer_thread_id], outer_thread_id, rank, numberOfNodes);
//             printSingleHT(conInfos[outer_thread_id]->hashTable[0], campaingCnt);
//                 printHT(conInfos[outer_thread_id]->hashTable, campaingCnt, outer_thread_id);
          }
       }
       cout << "finished, sending finish buffer " << getTimestamp() << endl;
       if(rank == 1)
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

    for(size_t n = 0; n < numaNodes; n++)
    {
        for(size_t i = 0; i < numberOfProducer/numaNodes; i++)
        {
            sumProducedTuples += producesTuples[n][i];
            sumProducedBuffer += producedBuffers[n][i];
            sumReadInTuples += readInputTuples[n][i];
            sumNoFreeEntry += noFreeEntryFound[n][i];
        }

        for(size_t i = 0; i < numberOfConsumer/numaNodes; i++)
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
    exit(0);
    return 0;
}
