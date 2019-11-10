#ifndef _IOT_MASTER_H
#define _IOT_MASTER_H

#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>

#include "../include/NodeEngine/NodeProperties.hpp"
#include "json.hpp"

using JSON = nlohmann::json;

class IotMaster {
  public:
    IotMaster(int parallelism) : _parallelism(parallelism), threadPool(parallelism)
    {
        assert(parallelism == 1);
        this->start();
    }

    static void* run(void* arg)
    {
        std::vector<void*> ptrs = *((std::vector<void*>*)arg);

        std::map<std::string, JSON> clientInfos = *((std::map<std::string, JSON>*)ptrs[0]);
        std::map<std::string, std::string> preCombinedBinaries = *((std::map<std::string, std::string>*)ptrs[1]);
        std::map<std::string, std::string> queryPlans = *((std::map<std::string, std::string>*)ptrs[2]);

        zmq::context_t context(1);
        zmq::socket_t socket(context, ZMQ_REP);
        socket.bind("tcp://*:5555");
        int mallocSize = 8096 * 10;
        char* dest = (char*)malloc(sizeof(char) * mallocSize);
        // char dest[8096] = { 0 };
        while (true) {
            zmq::message_t request;
            socket.recv(&request);
            if (request.size() > mallocSize) {
                mallocSize = request.size();
                free(dest);
                dest = (char*)malloc(sizeof(char) * mallocSize);
            }
            memcpy(dest, request.data(), request.size());
            dest[request.size()] = '\0';

            JSON data = JSON::parse(dest);
            // std::cout << data << std::endl;
            auto it = data.begin();
            auto key = it.key();

            // JSON val = it.value();
            std::cout << data << std::endl;
            // for (auto it = data.begin(); it != data.end(); it ++) {
            {
                JSON val = it.value();
                std::cout << val << std::endl;
                std::cout << "========================================" << std::endl;
                if (val.find("cpus") != val.end() || val.find("fs") != val.end() || val.find("mem") != val.end() ||
                    val.find("nets") != val.end()) {
                    clientInfos[it.key()] = it.value();
                }
                if (val.find("precombinedbinary") != val.end()) {

                    preCombinedBinaries[it.key()] = val["precombinedbinary"];
                }
                if (val.find("queryplan") != val.end()) {
                    queryPlans[it.key()] = val["queryplan"];
                }
            }
            std::cout << "receive data size: " << request.size() << std::endl;
            std::cout << "clientInfos.size: " << clientInfos.size() << std::endl;
            std::cout << "preCombinedBinardy.size: " << preCombinedBinaries.size() << std::endl;
            std::cout << "queryPlans.size: " << queryPlans.size() << std::endl;

            zmq::message_t reply(5);
            memcpy(reply.data(), "World", 5);
            socket.send(reply);
        }
    }
    void start()
    {
        ptrs.push_back((void*)&this->clientInfos);
        ptrs.push_back((void*)&this->preCombinedBinaries);
        ptrs.push_back((void*)&this->queryPlans);

        int rc = pthread_create(&threadPool[0], NULL, run, (void*)(&ptrs));
        assert(rc == 0);
        std::cout << "client info collector threads fired up..." << std::endl;
    }

    // static void *runPreCombinedBinaryReceiver(void *arg) {
    //   std::map<std::string, std::string> precombinedBinaries = *((std::map<std::string, JSON> *)arg);
    //   zmq::context_t context (1);
    //   zmq::socket_t socket (context, ZMQ_REP);
    //   socket.bind("tcp://*:5556");
    //   char dest[8096] = {0};
    //   while (true) {
    //     zmq::message_t request;
    //     socket.recv(&request);
    //     memcpy(dest, request.data(), request.size());
    //     dest[request.size()] = '\0';
    //     // FORMAT: {"client-id": <> }
    //     // JSON clientInfo = JSON::parse(dest);

    //     for (auto it = clientInfo.begin(); it != clientInfo.end(); it ++) {
    //       clientInfos[it.key()] = it.value();
    //     }
    //     std::cout << "clientInfos.size: " << clientInfos.size() << std::endl;
    //   }
    // }
    // void startPreCombinedBinaryReceiver() {
    //   int rc = pthread_create (&threadPool[1], NULL, runPreCombinedBinaryReceiver, (void *)(&this->clientInfos));
    //   assert(rc == 0);
    //   std::cout << "pre-comibined binary receiver threads fired up..." << std::endl;
    // }

    // static void *runQueryPlanReceiver(void *arg) {

    // }
    // void startQueryPlanReceiver() {
    //   int rc = pthread_create (&threadPool[2], NULL, runQueryPlanReceiver, (void *)(&this->queryPlans));
    //   assert(rc == 0);
    //   std::cout << "query planreceiver threads fired up..." << std::endl;
    // }
    void serve()
    {
        do {
        } while (true);
    }

  private:
    int _parallelism;
    std::vector<pthread_t> threadPool;

    std::map<std::string, JSON> clientInfos; // node information
    std::map<std::string, std::string> preCombinedBinaries;
    std::map<std::string, std::string> queryPlans;
    std::vector<void*> ptrs;
};

#endif // _IOT_MASTER_H
