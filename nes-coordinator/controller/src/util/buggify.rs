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

//! Fault-injection points for the madsim simulation. The macros expand to the
//! simulator's buggify predicate under `cfg(madsim)` and to nothing (or a
//! constant false) otherwise, so the guarded code never runs in normal builds.

/// Evaluates to the madsim buggify predicate, or to `false` in normal builds.
/// Use in an `if` when the fault needs a statement other than a plain return,
/// for example `if buggify!() { continue; }`.
#[cfg(madsim)]
macro_rules! buggify {
    () => {
        tokio::madsim::buggify::buggify()
    };
}

#[cfg(not(madsim))]
macro_rules! buggify {
    () => {
        false
    };
}

pub(crate) use buggify;

/// Returns from the enclosing function when the madsim buggify predicate fires,
/// and expands to nothing in normal builds. With no argument it returns `()`;
/// with an argument it returns that value.
#[cfg(madsim)]
macro_rules! buggify_return {
    () => {
        if tokio::madsim::buggify::buggify() {
            return;
        }
    };
    ($value:expr) => {
        if tokio::madsim::buggify::buggify() {
            return $value;
        }
    };
}

#[cfg(not(madsim))]
macro_rules! buggify_return {
    () => {};
    ($value:expr) => {};
}

pub(crate) use buggify_return;
