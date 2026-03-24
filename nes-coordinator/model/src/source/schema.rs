#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, EnumIter)]
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Nullable {
    #[serde(rename = "IS_NULLABLE")]
    IsNullable,
    #[serde(rename = "NOT_NULLABLE")]
    NotNullable,
}

pub type FieldName = String;

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeField {
    #[cfg_attr(feature = "testing", proptest(strategy = "field_name_strategy()"))]
    pub name: FieldName,
    #[serde(rename = "type")]
    #[cfg_attr(
        feature = "testing",
        proptest(strategy = "arb_data_type_pair()")
    )]
    pub data_type: (DataType, Nullable),
}

#[cfg(feature = "testing")]
fn arb_data_type_pair() -> impl proptest::strategy::Strategy<Value = (DataType, Nullable)> {
    use proptest::prelude::*;
    any::<DataType>().prop_map(|dt| (dt, Nullable::NotNullable))
}

#[cfg(feature = "testing")]
fn field_name_chars() -> impl proptest::strategy::Strategy<Value = char> {
    use proptest::prelude::*;
    prop_oneof![
        proptest::char::range('a', 'z'),
        proptest::char::range('0', '9'),
        Just('_')
    ]
}

#[cfg(feature = "testing")]
fn field_name_strategy() -> impl proptest::strategy::Strategy<Value = String> {
    use proptest::prelude::*;
    (
        proptest::char::range('a', 'z'),
        proptest::collection::vec(field_name_chars(), 0..=19),
    )
        .prop_map(|(first, rest)| std::iter::once(first).chain(rest).collect::<String>())
}

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct Schema {
    #[cfg_attr(
        feature = "testing",
        proptest(strategy = "proptest::collection::vec(\
            proptest::prelude::any::<AttributeField>(),\
            1..10)")
    )]
    fields: Vec<AttributeField>,
}

impl Schema {
    pub fn from_fields(fields: Vec<AttributeField>) -> Schema {
        assert!(
            !fields.is_empty(),
            "Cannot construct Schema with empty fields"
        );
        Self { fields }
    }
}
