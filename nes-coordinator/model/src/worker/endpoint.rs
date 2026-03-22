use std::fmt;
use std::str::FromStr;

pub type HostName = String;
pub type HostAddr = NetworkAddr;
pub type GrpcAddr = NetworkAddr;

pub const DEFAULT_GRPC_PORT: u16 = 8080;
pub const DEFAULT_DATA_PORT: u16 = 9090;

use sea_orm::sea_query::{ArrayType, StringLen, ValueTypeErr};
use sea_orm::{
    ColIdx, ColumnType, DbErr, QueryResult, TryFromU64, TryGetError, TryGetable, Value, sea_query,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct NetworkAddr {
    pub host: HostName,
    pub port: u16,
}

impl NetworkAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();
        assert!(!host.is_empty(), "Hostname cannot be empty");
        assert!(port > 0, "Port cannot be 0");
        Self { host, port }
    }

    pub fn to_uri(&self, scheme: &str) -> http::Uri {
        http::Uri::builder()
            .scheme(scheme)
            .authority(format!("{}:{}", self.host, self.port))
            .path_and_query("/")
            .build()
            .expect("invalid NetworkAddr components")
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

impl sea_query::ValueType for NetworkAddr {
    fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
        match v {
            Value::String(Some(x)) => x.parse().map_err(|_| sea_query::ValueTypeErr),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        todo!()
    }

    fn array_type() -> ArrayType {
        todo!()
    }

    fn column_type() -> ColumnType {
        sea_query::ColumnType::String(StringLen::None)
    }
}

impl TryFromU64 for NetworkAddr {
    fn try_from_u64(_n: u64) -> Result<Self, DbErr> {
        Err(DbErr::Custom("NetworkAddr is not numeric".to_owned()))
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
        let s = s
            .strip_prefix("http://")
            .or_else(|| s.strip_prefix("https://"))
            .unwrap_or(s);
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

impl<'a> From<&'a str> for NetworkAddr {
    fn from(s: &'a str) -> Self {
        NetworkAddr::from_str(s).expect("invalid NetworkAddr string")
    }
}

#[cfg(feature = "testing")]
fn hostname_chars() -> impl proptest::strategy::Strategy<Value = char> {
    use proptest::prelude::*;
    prop_oneof![proptest::char::range('a', 'z'), proptest::char::range('0', '9')]
}

#[cfg(feature = "testing")]
impl crate::Generate for NetworkAddr {
    fn generate() -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::*;
        let host_name = prop_oneof![
            Just("localhost".to_string()),
            Just("127.0.0.1".to_string()),
            (0..255u8, 0..255u8, 0..255u8, 0..255u8)
                .prop_map(|(a, b, c, d)| format!("{}.{}.{}.{}", a, b, c, d)),
            proptest::collection::vec(hostname_chars(), 1..=10)
                .prop_map(|chars| chars.into_iter().collect::<String>())
        ];
        (host_name, 1024..65535u16)
            .prop_map(|(host, port)| NetworkAddr::new(host, port))
            .boxed()
    }
}
