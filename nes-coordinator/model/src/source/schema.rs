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

use proptest::collection::vec;
use proptest_derive::Arbitrary;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

#[derive(Arbitrary, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
pub enum DataType {
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT32,
    FLOAT64,
    BOOLEAN,
    CHAR,
    VARSIZED,
}

#[derive(Arbitrary, Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Nullable {
    #[serde(rename = "IS_NULLABLE")]
    IsNullable,
    #[serde(rename = "NOT_NULLABLE")]
    NotNullable,
}

#[derive(Arbitrary, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeField {
    #[proptest(regex = "[a-z][a-z0-9_]{0,19}")]
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: (DataType, Nullable),
}

#[derive(Arbitrary, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct Schema {
    #[proptest(strategy = "vec(proptest::prelude::any::<AttributeField>(), 1..10)")]
    fields: Vec<AttributeField>,
}
