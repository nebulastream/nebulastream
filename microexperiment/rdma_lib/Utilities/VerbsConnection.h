//
// Created by Toso, Lorenzo on 2018-12-04.
//

#pragma once

#include <infinity/infinity.h>
#include <memory>
#include <vector>
#include "ConnectionInfoProvider/ConnectionInfoProvider.h"

using ReceiveElement = infinity::core::receive_element_t;
using infinity::memory::Buffer;
using infinity::requests::RequestToken;
using infinity::memory::RegionToken;

class VerbsConnection {

private:
    size_t target_rank;
    infinity::core::Context * context;
    infinity::queues::QueuePair * qp;
    infinity::queues::QueuePairFactory * qp_factory;

    char send_barrier_counter = 1;
    char recv_barrier_counter = 1;
    Buffer * send_barrier_buffer;
    Buffer * recv_barrier_buffer;
    RegionToken * remote_barrier_region_token;

    bool is_receiving = false;




public:
    infinity::memory::Atomic* getAtomic()
    {
        return new infinity::memory::Atomic(context);
    }
    Buffer * preregistered_size_buffer;
    Buffer * preregistered_region_token_buffer;
    explicit VerbsConnection(const ConnectionInfoProvider * infoProvier);

    VerbsConnection();

    virtual ~VerbsConnection();

    void atomic_cas(RegionToken * remote_token, int64_t compare, int64_t set);
    bool atomic_cas_blocking(RegionToken * remote_token, int64_t compare, int64_t set, RequestToken * pRequestToken);
    size_t atomic_cas_blocking(RegionToken* remote_token, size_t offset, int64_t compare, int64_t set, RequestToken* pRequestToken);


    bool check_receive(infinity::core::receive_element_t & receive_element);
    void wait_for_receive(infinity::core::receive_element_t & receive_element);

    void send(Buffer * buffer, RequestToken * requestToken = nullptr);
    void send_blocking(Buffer * buffer, RequestToken * requestToken = nullptr);
    void send(Buffer* buffer, size_t localOffset, size_t size, RequestToken * pRequestToken = nullptr);
    void post_receive(Buffer * buffer);
    void post_and_receive_blocking(Buffer* buffer);

    void compareAndSwap(infinity::memory::RegionToken* destination, infinity::memory::Atomic* previousValue, uint64_t compare, uint64_t swap);

    template <typename T>
    T receive() {
        ReceiveElement element;

        T obj;
        auto buffer = register_buffer(&obj, sizeof(T));
        post_receive(buffer);
        barrier();
        wait_for_receive(element);

        delete (buffer);
        return obj;
    }

    template <typename T>
    void send(const T & obj) {
        auto buffer = register_buffer(const_cast<T*>(&obj), sizeof(T));
        barrier();
        send_blocking(buffer);
        delete (buffer);
    }

    void read(Buffer* destination, RegionToken * source, RequestToken * requestToken = nullptr);
    void read_blocking(Buffer* destination, RegionToken * source, RequestToken * requestToken = nullptr);
    void read_blocking(Buffer * buffer, RegionToken * remote_token, size_t local_offset, size_t remote_offset, size_t size, RequestToken * pRequestToken = nullptr);
    void write(Buffer * buffer, RegionToken * remote_token, RequestToken * pRequestToken = nullptr);
    void write_blocking(Buffer * buffer, RegionToken * remote_token, RequestToken * pRequestToken = nullptr);
    void write(Buffer * buffer, RegionToken * remote_token, size_t local_offset, size_t remote_offset, size_t size, RequestToken * pRequestToken = nullptr);
    void read(Buffer * buffer, RegionToken * remote_token, size_t local_offset, size_t remote_offset, size_t size, RequestToken * pRequestToken = nullptr);
    void write_blocking(Buffer * buffer, RegionToken * remote_token, size_t local_offset, size_t remote_offset, size_t size, RequestToken * pRequestToken = nullptr);
    std::shared_ptr<RegionToken> exchange_region_tokens(Buffer * buffer = nullptr);

    void barrier();
    void wait_for_notification();
    void send_notification();

    Buffer * allocate_buffer(size_t size);
    Buffer * register_buffer(void * ptr, size_t size);
    RequestToken * create_request_token();


    void share_data(void* data, size_t element_count, size_t element_size);
    void read_data(void* destination, size_t element_count, size_t element_size);

    void send_buffered(size_t buffer_size, void* data, size_t element_count, size_t element_size);
    void recv_buffered(size_t buffer_size, void* data, size_t element_count, size_t element_size);
    void send_buffered(Buffer* buffer, void* data, size_t element_count, size_t element_size);
    void recv_buffered(std::vector<Buffer*> & buffers, void* data, size_t element_count, size_t element_size);

    bool is_server() const;
    size_t get_target_rank() const;
private:
    void connect(const ConnectionInfoProvider &infoProvider);
    void disconnect();


    void accept_connection(const ConnectionInfoProvider &infoProvider, RegionToken* regionToken);

    void connect_to_remote(const ConnectionInfoProvider &infoProvider, RegionToken* regionToken);

    void perform_connection_loop(const ConnectionInfoProvider &infoProvider, RegionToken* regionToken, uint retries);

    void receive_test_message(infinity::memory::Buffer* buffer);
    void send_test_message(infinity::memory::Buffer* buffer);

};



