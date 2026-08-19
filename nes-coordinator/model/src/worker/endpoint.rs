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

//! The network address type used to identify and reach a worker, with parsing
//! and validation.

use anyhow::Result;
use sea_orm::sea_query::{ArrayType, Nullable, StringLen, ValueType, ValueTypeErr};
use sea_orm::{ColIdx, ColumnType, DbErr, QueryResult, TryFromU64, TryGetError, TryGetable, Value};
use std::fmt;
use std::str::FromStr;

#[cfg(any(test, feature = "testing"))]
pub const DEFAULT_HOST_PORT: u16 = 8080;
#[cfg(any(test, feature = "testing"))]
pub const DEFAULT_DATA_PORT: u16 = 9090;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkAddr {
    pub host: String,
    pub port: u16,
}

impl NetworkAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self> {
        let host = host.into();
        if host.is_empty() {
            anyhow::bail!("Hostname cannot be empty")
        }
        if port == 0 {
            anyhow::bail!("Port cannot be zero")
        }
        Ok(Self { host, port })
    }
}

impl fmt::Display for NetworkAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Bracket IPv6 literals so the host/port separator stays unambiguous.
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for NetworkAddr {
    type Err = String;

    /// Accepts `host:port` (hostname or IPv4) and the bracketed form
    /// `[host]:port` required for IPv6 literals. A bare host that itself
    /// contains a colon (an unbracketed IPv6 address) is rejected rather
    /// than mis-split on its last colon.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (host, port_str) = if let Some(rest) = s.strip_prefix('[') {
            let close = rest.find(']').ok_or("Missing closing ']' in address")?;
            let after = rest[close + 1..]
                .strip_prefix(':')
                .ok_or("Expected ':port' after ']'")?;
            (&rest[..close], after)
        } else {
            let colon = s.rfind(':').ok_or("Missing port separator")?;
            let host = &s[..colon];
            if host.contains(':') {
                return Err("Unbracketed IPv6 address; wrap the host in [ ]".to_string());
            }
            (host, &s[colon + 1..])
        };
        let port: u16 = port_str
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?;
        // Delegate the empty-host / zero-port checks to `new` so the rules
        // live in one place.
        NetworkAddr::new(host, port).map_err(|e| e.to_string())
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

impl Nullable for NetworkAddr {
    fn null() -> Value {
        Value::String(None)
    }
}

impl TryFromU64 for NetworkAddr {
    fn try_from_u64(_n: u64) -> Result<Self, DbErr> {
        Err(DbErr::Custom("NetworkAddr is not numeric".to_owned()))
    }
}

#[cfg(any(test, feature = "testing"))]
mod arbitrary;

#[cfg(test)]
mod tests {
    use super::NetworkAddr;

    #[test]
    fn round_trips_all_forms() {
        for s in [
            "localhost:8080",
            "1.2.3.4:9090",
            "[::1]:8080",
            "[2001:db8::1]:443",
        ] {
            let addr: NetworkAddr = s.parse().expect("should parse");
            assert_eq!(addr.to_string(), s);
        }
    }

    #[test]
    fn rejects_unbracketed_ipv6() {
        assert!("fe80::1".parse::<NetworkAddr>().is_err());
        assert!("::1:8080".parse::<NetworkAddr>().is_err());
    }

    #[test]
    fn rejects_empty_host_and_zero_port() {
        assert!(":8080".parse::<NetworkAddr>().is_err());
        assert!("host:0".parse::<NetworkAddr>().is_err());
    }
}
