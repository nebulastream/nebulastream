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

use nes_network::registry;
use nes_network::sender::SenderConfig;

#[cxx::bridge]
pub mod ffi {
    /// Configuration for network services (sender and receiver).
    /// Passed once during initialization and applied to all channels on this worker.
    struct NetworkServiceOptions {
        /// Size of the sender software queue per channel (default: 1024).
        sender_queue_size: u32,
        /// Maximum number of in-flight buffers awaiting acknowledgment per channel (default: 64).
        max_pending_acks: u32,
        /// Size of the receiver data queue per channel (default: 10).
        receiver_queue_size: u32,
    }

    extern "Rust" {
        fn enable_memcom();
        fn init_receiver_service(
            connection_addr: String,
            io_runtime_idx: usize,
            options: &NetworkServiceOptions,
        ) -> Result<()>;
        fn init_sender_service(
            connection_addr: String,
            io_runtime_idx: usize,
            options: &NetworkServiceOptions,
        ) -> Result<()>;
    }
}

fn enable_memcom() {
    registry::enable_memcom();
}

fn init_sender_service(
    this_connection_addr: String,
    io_runtime_idx: usize,
    options: &ffi::NetworkServiceOptions,
) -> Result<(), String> {
    let handle = nes_io_runtime::get_handle(io_runtime_idx)
        .ok_or_else(|| "IORuntime not initialized".to_string())?;
    registry::init_sender_service(
        &this_connection_addr,
        &handle,
        SenderConfig {
            sender_queue_size: options.sender_queue_size as usize,
            max_pending_acks: options.max_pending_acks as usize,
        },
    )
}

fn init_receiver_service(
    connection_addr: String,
    io_runtime_idx: usize,
    options: &ffi::NetworkServiceOptions,
) -> Result<(), String> {
    let handle = nes_io_runtime::get_handle(io_runtime_idx)
        .ok_or_else(|| "IORuntime not initialized".to_string())?;
    registry::init_receiver_service(
        &connection_addr,
        &handle,
        options.receiver_queue_size as usize,
    )
}
