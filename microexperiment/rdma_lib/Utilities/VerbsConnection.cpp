#include <memory>

#include <memory>

//
// Created by Toso, Lorenzo on 2018-12-04.
//

#include <infinity/infinity.h>
#include <zconf.h>
#include "VerbsConnection.h"
#include "MPIHelper.h"
#include "Debug.h"
#include <atomic>
#include "JoinParameters.h"
#include <string>
#include "string.h"
using namespace std;

VerbsConnection::VerbsConnection(const ConnectionInfoProvider * infoProvider)
:target_rank(infoProvider->get_target_rank()) {
    connect(*infoProvider);
}

void VerbsConnection::connect(const ConnectionInfoProvider &infoProvider) {
    this->context = new infinity::core::Context(infoProvider.get_device_index(), infoProvider.get_device_port());
    this->qp_factory = new infinity::queues::QueuePairFactory(context);


    this->send_barrier_buffer = allocate_buffer(1);
    ((char*)this->send_barrier_buffer->getData())[0] = (char)MPIHelper::get_rank();
    this->recv_barrier_buffer = allocate_buffer(1);
    auto regionToken = this->recv_barrier_buffer->createRegionToken();

    this->preregistered_region_token_buffer = allocate_buffer(sizeof(RegionToken));
    this->preregistered_size_buffer = allocate_buffer(sizeof(size_t));

    perform_connection_loop(infoProvider, regionToken, 50000);

    this->remote_barrier_region_token = static_cast<RegionToken*>(this->qp->getUserData());
    delete regionToken;
    TRACE("Received a memory_region_token for a region of size %lu\n", this->remote_barrier_region_token->getSizeInBytes());

}

void VerbsConnection::perform_connection_loop(const ConnectionInfoProvider &infoProvider, RegionToken* regionToken, uint retries) {

    is_receiving = MPIHelper::get_rank() < target_rank;

    for(uint i = 0; i < retries; i++){
        if(is_receiving){
            accept_connection(infoProvider, regionToken);
        }
        else {
            connect_to_remote(infoProvider, regionToken);
        }
        if(this->qp != nullptr) {
            TRACE("Connected\n");
            break;
        }
        sleep(1);
    }
    if(this->qp == nullptr)
    {
        fprintf(stderr, "Could not establish a connection to rank %lu\n", infoProvider.get_target_rank());
        exit(1);
    }
    else {
        auto b = allocate_buffer(sizeof(size_t));
        if(is_receiving){
            receive_test_message(b);
            send_test_message(b);
        } else {
            send_test_message(b);
            receive_test_message(b);
        }
        delete b;
    }
}

void VerbsConnection::send_test_message(Buffer* buffer) {
    ((char*) buffer->getData())[0] = static_cast<char>(MPIHelper::get_rank());
    usleep(100 * 1000);
    send_blocking(buffer);
}

void VerbsConnection::receive_test_message(Buffer* buffer) {
    post_receive(buffer);
    infinity::core::receive_element_t r;
    wait_for_receive(r);
    char received_value = ((char*) (r.buffer->getData()))[0];

    if (received_value == static_cast<char>(target_rank))
    {
        TRACE("Test-Message %lu->%lu successful\n", target_rank, MPIHelper::get_rank());
    }
    else {
        fprintf(stderr, "Something weird happened on test-message %lu->%lu\n", target_rank, MPIHelper::get_rank());
        throw "error";
    }

}


void VerbsConnection::connect_to_remote(const ConnectionInfoProvider &infoProvider, RegionToken* regionToken) {
    usleep(750 * 1000);
    u_int16_t port = infoProvider.get_connection_port();
    std::__cxx11::string ip = infoProvider.get_target_ip();
    TRACE("Connecting.\n");
    qp = qp_factory->connectToRemoteHost(ip.c_str(), port, regionToken, sizeof(RegionToken));
}

void VerbsConnection::accept_connection(const ConnectionInfoProvider &infoProvider, RegionToken* regionToken) {
    TRACE("Waiting for incoming connection\n");
    qp_factory->bindToPort(infoProvider.get_connection_port());
    qp = qp_factory->acceptIncomingConnection(regionToken, sizeof(RegionToken));
    TRACE("Connected\n");
}

void VerbsConnection::send(Buffer* buffer, RequestToken * pRequestToken) {
    qp->send(buffer, pRequestToken);
}
void VerbsConnection::send(Buffer* buffer, size_t localOffset, size_t size, RequestToken * pRequestToken) {
    qp->send(buffer, localOffset, static_cast<uint32_t>(size), infinity::queues::OperationFlags(), pRequestToken);
}

void VerbsConnection::send_blocking(Buffer* buffer, RequestToken * pRequestToken) {
    if(pRequestToken == nullptr){
        infinity::requests::RequestToken requestToken(context);
        qp->send(buffer, &requestToken);
        requestToken.waitUntilCompleted();
    }
    else {
        qp->send(buffer, pRequestToken);
        pRequestToken->waitUntilCompleted();
    }

}

void VerbsConnection::post_receive(Buffer* buffer) {
    context->postReceiveBuffer(buffer);
}

void VerbsConnection::wait_for_receive(ReceiveElement & receive_element){
    while (!context->receive(&receive_element));
}

bool VerbsConnection::check_receive(ReceiveElement & receiveElement) {
    return context->receive(&receiveElement);
}

void VerbsConnection::post_and_receive_blocking(Buffer* buffer) {
    ReceiveElement receiveElement;
    context->postReceiveBuffer(buffer);

    while (!context->receive(&receiveElement));

}

RequestToken * VerbsConnection::create_request_token(){
    return new RequestToken(context);
}
infinity::memory::Buffer* VerbsConnection::allocate_buffer(size_t size) {
    return new infinity::memory::Buffer(context, size);
}
infinity::memory::Buffer* VerbsConnection::register_buffer(void* ptr, size_t size) {
    TRACE("Registering buffer.\n");
    return new infinity::memory::Buffer(context, ptr, size);
}

void VerbsConnection::disconnect() {
    TRACE("delete barrier_buffer\n");
    delete recv_barrier_buffer;
    delete send_barrier_buffer;

    delete preregistered_size_buffer;
    delete preregistered_region_token_buffer;

    TRACE("delete qp\n");
    delete qp;
    TRACE("delete qp_factory\n");
    delete qp_factory;
    TRACE("delete context\n");
    delete context;
    TRACE("Done deleting\n");
}

VerbsConnection::~VerbsConnection() {
    disconnect();
}

void VerbsConnection::barrier() {
    //TRACE2("Entering barrier\n");

    send_notification();
    wait_for_notification();
    //recv_barrier_counter

    //TRACE("WAITING FOR RDMA FLAG\n");
    //TRACE("RDMA FLAG RECEIVED\n");
    //TRACE("SENDING RDMA FLAG\n");
    //TRACE("RDMA FLAG SENT\n");

    //TRACE2("Finished barrier\n");
}

void VerbsConnection::wait_for_notification() {
    //TRACE("wait_for_notification %d\n", recv_barrier_counter);
    while(((volatile char*)recv_barrier_buffer->getData())[0] < recv_barrier_counter);
    recv_barrier_counter++;
    //TRACE("wait_for_rdma_flag completed\n");

}

void VerbsConnection::send_notification() {
    //TRACE("send_notification %d\n", send_barrier_counter);

    RequestToken token(context);
    ((char*)send_barrier_buffer->getData())[0] = send_barrier_counter;
    qp->write(send_barrier_buffer, remote_barrier_region_token, 1, &token);
    //TRACE("Write of flag %d started\n", ((char*)local_buffer->getData())[0]);
    token.waitUntilCompleted();
    send_barrier_counter++;
    //((char*)send_barrier_buffer->getData())[0] = static_cast<char>(255);
    //TRACE("Write of flag completed\n");

}

bool VerbsConnection::is_server() const {
    return is_receiving;
}

std::shared_ptr<RegionToken> VerbsConnection::exchange_region_tokens(Buffer* buffer) {

    size_t token_size = sizeof(RegionToken);
    auto exchange_buffer = allocate_buffer(token_size);
    infinity::core::receive_element_t receiveElement;

    auto return_value = std::make_shared<RegionToken>();

    if ( MPIHelper::get_rank() < target_rank ) {
        if(buffer != nullptr){
            auto region_token = buffer->createRegionToken();
        }
        usleep(50*1000);
        send_blocking(exchange_buffer);

        memset(exchange_buffer->getData(), 0, token_size);
        post_receive(exchange_buffer);
        wait_for_receive(receiveElement);
        memcpy(return_value.get(), exchange_buffer->getData(), token_size);
    } else {
        post_receive(exchange_buffer);
        wait_for_receive(receiveElement);
        memcpy(return_value.get(), exchange_buffer->getData(), token_size);

        if(buffer != nullptr){
            auto region_token = buffer->createRegionToken();
            memcpy(exchange_buffer->getData(), region_token, token_size);
        }
        usleep(50*1000);
        send_blocking(exchange_buffer);
    }
    
    bool is_valid = false;
    for(size_t i = 0; i < token_size; i++){
        if(((char*)exchange_buffer->getData())[i] != 0)
        {
            is_valid = true;
            break;
        }
    }


    if(is_valid){
        TRACE("Received valid region token\n");
    }
    else {
        TRACE("Received invalid region token\n");
    }

    delete exchange_buffer;
    return return_value;
}

void VerbsConnection::write(Buffer* buffer, RegionToken* remote_token, RequestToken* pRequestToken) {
    qp->write(buffer, remote_token, pRequestToken);
}

void VerbsConnection::write_blocking(Buffer* buffer, RegionToken* remote_token, RequestToken* pRequestToken) {
    if(pRequestToken != nullptr){
        qp->write(buffer, remote_token, pRequestToken);
        pRequestToken->waitUntilCompleted();
    }
    else {
        auto r = create_request_token();
        qp->write(buffer, remote_token, r);
        r->waitUntilCompleted();
        delete r;
    }
}

size_t VerbsConnection::get_target_rank() const {
    return target_rank;
}

VerbsConnection::VerbsConnection() {}

void VerbsConnection::recv_buffered(std::vector<Buffer*> & buffers, void* data, size_t element_count, size_t element_size) {

    for( auto & buffer : buffers)
        post_receive(buffer);
    //TRACE("Sending notification\n");
    barrier();

    size_t total_received_element_count = 0;
    ReceiveElement receiveElement;
    while (total_received_element_count < element_count) {
        wait_for_receive(receiveElement);

        size_t * count_ptr = (size_t*)receiveElement.buffer->getData();
        size_t * value_ptr = count_ptr + 1;

        size_t recv_element_count = *count_ptr;
        memcpy(((char*)data + total_received_element_count * element_size),  value_ptr, recv_element_count * element_size);
        post_receive(receiveElement.buffer);

        total_received_element_count += recv_element_count;
        //TRACE("Received %lu elements\n", recv_element_count);
    }

    //TRACE("Done receiving, now waiting for EOF-Messages.\n");
    for(size_t i = 0; i < buffers.size(); i++){
        wait_for_receive(receiveElement);
        //TRACE("Received EOF message %lu\n", i);
        //TRACE("EOF message contained: %lu\n", *((size_t*)receiveElement.buffer->getData()));
    }
    //con(sending_rank).barrier();

}
void VerbsConnection::send_buffered(Buffer* buffer, void* data, size_t element_count, size_t element_size) {
    size_t buffer_size = buffer->getSizeInBytes();
    RequestToken* request_token = create_request_token();

    size_t max_elements_per_buffer = (buffer_size - sizeof(size_t)) / element_size;

    size_t * count_ptr = (size_t*)buffer->getData();
    size_t * value_ptr = count_ptr + 1;



    size_t sent_elements = 0;

    //TRACE("Waiting for notification\n");
    barrier();
    //TRACE("Received notification\n");

    while (sent_elements < element_count) {
        size_t elements_in_this_buffer = std::min(max_elements_per_buffer, element_count - sent_elements);

        *count_ptr = elements_in_this_buffer;
        memcpy(value_ptr, ((char*)data) + sent_elements * element_size,  elements_in_this_buffer * element_size);

        send_blocking(buffer, request_token); // todo does not have to be blocking

        sent_elements += elements_in_this_buffer;
        //TRACE("Sent %lu elements\n", elements_in_this_buffer);
    }

    *count_ptr = 0;
    for(size_t i = 0; i < MOVE_ALL_TUPLES_BUFFER_COUNT; i++){   //todo pretty sure this actually needs to be a parameter that is passed
        //TRACE("Sending EOF message %lu\n", i);
        send_blocking(buffer);
        //TRACE("EOF message contained: %lu\n", *((size_t*)buffer->getData()));
    }
    //con(target_rank).barrier();
}



void VerbsConnection::send_buffered(size_t buffer_size, void* data, size_t element_count, size_t element_size) {
    auto buffer = allocate_buffer(buffer_size);
    send_buffered(buffer, data, element_count, element_size);
    delete buffer;
}

void VerbsConnection::recv_buffered(size_t buffer_size, void* data, size_t element_count, size_t element_size) {

    std::vector<Buffer*> buffers(MOVE_ALL_TUPLES_BUFFER_COUNT);
    for(size_t i = 0; i < MOVE_ALL_TUPLES_BUFFER_COUNT; i++)
        buffers[i] = allocate_buffer(buffer_size);

    recv_buffered(buffers, data, element_count, element_size);

    for(auto & buffer : buffers)
        delete buffer;
}


void VerbsConnection::share_data(void* data, size_t element_count, size_t element_size) {
    auto buffer = register_buffer(data, element_size*element_count);
    auto region_token = buffer->createRegionToken();
    memcpy(data, region_token, sizeof(RegionToken));
    send(buffer, 0, sizeof(RegionToken));
    wait_for_notification();
    delete region_token;
    delete buffer;
}

void VerbsConnection::read_data(void* destination, size_t element_count, size_t element_size) {
    auto region_token_buffer = allocate_buffer(sizeof(RegionToken));
    post_receive(region_token_buffer);
    auto data_buffer = register_buffer(destination, element_count * element_size);

    auto requestToken = create_request_token();
    ReceiveElement receiveElement;
    wait_for_receive(receiveElement);
    qp->read(data_buffer, (RegionToken*)region_token_buffer->getData(), requestToken);
    requestToken->waitUntilCompleted();
    send_notification();
    delete region_token_buffer;
    delete data_buffer;
}

void VerbsConnection::read(Buffer* destination, RegionToken * source, RequestToken * requestToken) {
    if(requestToken == nullptr)
        qp->read(destination, source);
    else
        qp->read(destination, source, requestToken);

}

void VerbsConnection::read_blocking(Buffer* destination, RegionToken* source, RequestToken* pRequestToken) {
    if(pRequestToken != nullptr){
        read(destination, source, pRequestToken);
        pRequestToken->waitUntilCompleted();
    }
    else {
        auto r = create_request_token();
        read(destination, source, r);
        r->waitUntilCompleted();
        delete r;
    }
}

void VerbsConnection::write(Buffer* buffer, RegionToken* remote_token, size_t local_offset, size_t remote_offset, size_t size, RequestToken* pRequestToken) {
    qp->write(buffer, local_offset, remote_token, remote_offset, size, infinity::queues::OperationFlags(), pRequestToken);
}

void VerbsConnection::write_blocking(Buffer* buffer, RegionToken* remote_token, size_t local_offset, size_t remote_offset, size_t size, RequestToken* pRequestToken) {
    if(pRequestToken != nullptr){
        write(buffer, remote_token, local_offset, remote_offset, size, pRequestToken);
        pRequestToken->waitUntilCompleted();
    }
    else {
        auto r = create_request_token();
        write(buffer, remote_token, local_offset, remote_offset, size, r);
        r->waitUntilCompleted();
        delete r;
    }
}
