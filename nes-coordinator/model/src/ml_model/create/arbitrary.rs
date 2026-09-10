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

impl Arbitrary for CreateMlModel {
    type Parameters = ();
    fn arbitrary_with(_: ()) -> Self::Strategy {
        ("[a-z][a-z0-9_]{2,29}", "/[a-z]{1,8}/[a-z]{1,8}\\.onnx")
            .prop_map(|(name, path)| Self {
                name,
                path,
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                imported: serde_json::json!({}),
                if_not_exists: false,
            })
            .boxed()
    }
    type Strategy = proptest::strategy::BoxedStrategy<Self>;
}
