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

use proptest::arbitrary::Arbitrary;
use proptest::strategy::BoxedStrategy;
use sea_orm::sea_query::{ArrayType, StringLen, ValueType, ValueTypeErr};
use sea_orm::{
    ColIdx, ColumnType, DbErr, QueryResult, TryFromU64, TryGetError, TryGetable, Value,
};
use std::fmt;
use std::str::FromStr;

pub const DEFAULT_HOST_PORT: u16 = 8080;
pub const DEFAULT_DATA_PORT: u16 = 9090;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkAddr {
    pub host: String,
    pub port: u16,
}

impl NetworkAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();
        assert!(!host.is_empty(), "Hostname cannot be empty");
        assert!(port > 0, "Port cannot be 0");
        Self { host, port }
    }
}

impl fmt::Display for NetworkAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for NetworkAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let colon = s.rfind(':').ok_or("Missing port separator")?;
        let host = &s[..colon];
        let port: u16 = s[colon + 1..]
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        if host.is_empty() {
            return Err("Hostname cannot be empty".to_string());
        }
        if port == 0 {
            return Err("Port cannot be 0".to_string());
        }
        Ok(NetworkAddr {
            host: host.to_string(),
            port,
        })
    }
}

impl serde::Serialize for NetworkAddr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for NetworkAddr {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl From<NetworkAddr> for Value {
    fn from(val: NetworkAddr) -> Self {
        Value::String(Some(Box::new(val.to_string())))
    }
}

impl TryGetable for NetworkAddr {
    fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let s: String = res.try_get_by(index)?;
        s.parse()
            .map_err(|e| TryGetError::DbErr(DbErr::Custom(format!("Parse error: {}", e))))
    }
}

impl ValueType for NetworkAddr {
    fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
        match v {
            Value::String(Some(x)) => x.parse().map_err(|_| ValueTypeErr),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        "NetworkAddr".to_string()
    }

    fn array_type() -> ArrayType {
        ArrayType::String
    }

    fn column_type() -> ColumnType {
        ColumnType::String(StringLen::None)
    }
}

impl TryFromU64 for NetworkAddr {
    fn try_from_u64(_n: u64) -> Result<Self, DbErr> {
        Err(DbErr::Custom("NetworkAddr is not numeric".to_owned()))
    }
}

impl Arbitrary for NetworkAddr {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        let host = prop_oneof![
            Just("localhost".to_string()),
            (0..255u8, 0..255u8, 0..255u8, 0..255u8)
                .prop_map(|(a, b, c, d)| format!("{a}.{b}.{c}.{d}")),
            "[a-z][a-z0-9]{0,9}".prop_map(String::from),
        ];
        (host, 1024..65535u16)
            .prop_map(|(host, port)| NetworkAddr::new(host, port))
            .boxed()
    }
}
