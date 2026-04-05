/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <NetworkRuntime.hpp>

#include <cstdint>

namespace NES
{

NetworkRuntime::NetworkRuntime(const std::string& connectionAddr, const std::string& workerId, const NetworkOptions& options)
    : sender(init_sender_service(
          rust::String(connectionAddr),
          rust::String(workerId),
          NetworkServiceOptions{
              .sender_queue_size = options.senderQueueSize,
              .max_pending_acks = options.maxPendingAcks,
              .receiver_queue_size = options.receiverQueueSize,
              .sender_io_threads = options.senderIOThreads,
              .receiver_io_threads = options.receiverIOThreads,
          }))
    , receiver(init_receiver_service(
          rust::String(connectionAddr),
          rust::String(workerId),
          NetworkServiceOptions{
              .sender_queue_size = options.senderQueueSize,
              .max_pending_acks = options.maxPendingAcks,
              .receiver_queue_size = options.receiverQueueSize,
              .sender_io_threads = options.senderIOThreads,
              .receiver_io_threads = options.receiverIOThreads,
          }))
{
}

NetworkRuntime::~NetworkRuntime() = default;

rust::Box<SenderDataChannel> NetworkRuntime::registerSenderChannel(
    const std::string& connectionAddr, const std::string& channelId, size_t senderQueueSize, size_t maxPendingAcks)
{
    const NetworkServiceOptions options{
        .sender_queue_size = static_cast<uint32_t>(senderQueueSize),
        .max_pending_acks = static_cast<uint32_t>(maxPendingAcks),
        .receiver_queue_size = 0,
    };
    return register_sender_channel(*sender, connectionAddr, rust::String(channelId), options);
}

rust::Box<ReceiverDataChannel> NetworkRuntime::registerReceiverChannel(const std::string& channelId, size_t receiverQueueSize)
{
    const NetworkServiceOptions options{
        .sender_queue_size = 0,
        .max_pending_acks = 0,
        .receiver_queue_size = static_cast<uint32_t>(receiverQueueSize),
    };
    return register_receiver_channel(*receiver, rust::String(channelId), options);
}

}
