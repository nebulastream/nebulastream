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

#[cxx::bridge]
pub mod fault_ffi {
    unsafe extern "C++" {
        #[allow(non_snake_case)]
        fn initActiveFaultContext(host: String);
        #[allow(non_snake_case)]
        fn checkIo() -> bool;
        #[allow(non_snake_case)]
        fn failpoint(name: &str) -> bool;
        fn deferredFailpoint(name: &str) -> u8;
        #[allow(non_snake_case)]
        fn applyFaultAction(action: u8);
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FaultAction {
    Crash,
    Disconnect,
    Udf,
}
#[inline]
#[cfg(feature = "fault-testing")]
#[macro_export]
macro_rules! init_fault_context {
    ($host:expr) => {
        $crate::fault_ffi::initActiveFaultContext($host.to_string())
    };
}

#[cfg(not(feature = "fault-testing"))]
#[macro_export]
macro_rules! init_fault_context {
    ($host:expr) => {
        ()
    };
}

#[cfg(feature = "fault-testing")]
#[macro_export]
macro_rules! check_io {
    () => {
        $crate::fault_testing::fault_ffi::checkIo()
    };
}

#[cfg(not(feature = "fault-testing"))]
#[macro_export]
macro_rules! check_io {
    () => {
        false
    };
}

#[cfg(feature = "fault-testing")]
#[macro_export]
macro_rules! failpoint {
    ($name:expr) => {
        $crate::fault_testing::fault_ffi::failpoint($name)
    };
}

#[cfg(not(feature = "fault-testing"))]
#[macro_export]
macro_rules! failpoint {
    ($name:expr) => {
        false
    };
}

#[cfg(feature = "fault-testing")]
#[macro_export]
macro_rules! deferred_failpoint {
    ($name:expr) => {
        match $crate::fault_testing::fault_ffi::deferredFailpoint($name) {
            0 => Some(FaultAction::Crash),
            1 => Some(FaultAction::Disconnect),
            2 => Some(FaultAction::Udf),
            _ => None,
        }
    };
}

#[cfg(not(feature = "fault-testing"))]
#[macro_export]
macro_rules! deferred_failpoint {
    ($name:expr) => {
        None::<FaultAction>
    };
}

#[cfg(feature = "fault-testing")]
#[macro_export]
macro_rules! apply_fault_action {
    ($action:expr) => {
        $crate::fault_testing::fault_ffi::applyFaultAction(match $action {
            FaultAction::Crash => 0,
            FaultAction::Disconnect => 1,
            FaultAction::Udf => 2,
        });
    };
}

#[cfg(not(feature = "fault-testing"))]
#[macro_export]
macro_rules! apply_fault_action {
    ($action:expr) => {
        ()
    };
}
