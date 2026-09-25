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

use super::*;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;

impl Arbitrary for NetworkAddr {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let host = prop_oneof![
            Just("localhost".to_string()),
            (0..255u8, 0..255u8, 0..255u8, 0..255u8)
                .prop_map(|(a, b, c, d)| format!("{a}.{b}.{c}.{d}")),
            "[a-z][a-z0-9]{0,9}".prop_map(String::from),
            (0..=0xffffu16, 0..=0xffffu16).prop_map(|(a, b)| format!("2001:db8::{a:x}:{b:x}")),
        ];
        (host, 1024..65535u16)
            .prop_map(|(host, port)| {
                NetworkAddr::new(host, port).expect("proptest inputs are valid")
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
