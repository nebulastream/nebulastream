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

use sea_orm::sea_query::{ArrayType, Nullable, ValueType, ValueTypeErr};
use sea_orm::{ColIdx, ColumnType, DbErr, QueryResult, TryFromU64, TryGetError, TryGetable, Value};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

/// Defines a strong type over `i64` for entity ids. Serialized and
/// stored as a plain integer; the wrapper exists only to prevent
/// cross-entity id mixups at compile time.
macro_rules! define_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(i64);

        impl $name {
            pub fn new(id: i64) -> Self {
                Self(id)
            }
        }

        impl Deref for $name {
            type Target = i64;
            fn deref(&self) -> &i64 {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<i64> for $name {
            fn from(id: i64) -> Self {
                Self(id)
            }
        }

        impl From<$name> for i64 {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(serializer)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                i64::deserialize(deserializer).map(Self)
            }
        }

        impl From<$name> for Value {
            fn from(val: $name) -> Self {
                Value::BigInt(Some(val.0))
            }
        }

        impl TryGetable for $name {
            fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
                let v: i64 = res.try_get_by(index)?;
                Ok(Self(v))
            }
        }

        impl ValueType for $name {
            fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
                match v {
                    Value::BigInt(Some(x)) => Ok(Self(x)),
                    Value::Int(Some(x)) => Ok(Self(x as i64)),
                    _ => Err(ValueTypeErr),
                }
            }

            fn type_name() -> String {
                stringify!($name).to_string()
            }

            fn array_type() -> ArrayType {
                ArrayType::BigInt
            }

            fn column_type() -> ColumnType {
                ColumnType::BigInteger
            }
        }

        impl Nullable for $name {
            fn null() -> Value {
                Value::BigInt(None)
            }
        }

        impl TryFromU64 for $name {
            fn try_from_u64(n: u64) -> Result<Self, DbErr> {
                Ok(Self(n as i64))
            }
        }
    };
}

define_id!(QueryId);
define_id!(FragmentId);
define_id!(SourceId);
define_id!(SinkId);
