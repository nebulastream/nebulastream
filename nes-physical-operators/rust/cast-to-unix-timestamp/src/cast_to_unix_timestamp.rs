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

use chrono::{DateTime, NaiveDateTime};

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn parse_timestamp_to_unix_milliseconds(timestamp: &str) -> Result<u64>;
    }
}

fn parse_timestamp_to_unix_milliseconds(timestamp: &str) -> Result<u64, String> {
    let timestamp = timestamp.trim();
    if timestamp.is_empty() {
        return Err("cannot convert an empty timestamp".to_owned());
    }

    let offset_formats = ["%Y-%m-%dT%H:%M:%S%:z", "%Y-%m-%dT%H:%M:%S%.f%:z"];
    let naive_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ];

    let parsed_milliseconds = offset_formats
        .iter()
        .find_map(|format| DateTime::parse_from_str(timestamp, format).ok())
        .map(|date_time| date_time.timestamp_millis())
        .or_else(|| {
            timestamp.strip_suffix('Z').and_then(|timestamp_without_z| {
                ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%.f"]
                    .iter()
                    .find_map(|format| {
                        NaiveDateTime::parse_from_str(timestamp_without_z, format).ok()
                    })
                    .map(|date_time| date_time.and_utc().timestamp_millis())
            })
        })
        .or_else(|| {
            NaiveDateTime::parse_from_str(timestamp, "%a, %d %b %Y %H:%M:%S GMT")
                .ok()
                .map(|date_time| date_time.and_utc().timestamp_millis())
        })
        .or_else(|| {
            naive_formats
                .iter()
                .find_map(|format| NaiveDateTime::parse_from_str(timestamp, format).ok())
                .map(|date_time| date_time.and_utc().timestamp_millis())
        })
        .ok_or_else(|| format!("unsupported timestamp format: '{timestamp}'"))?;

    u64::try_from(parsed_milliseconds)
        .map_err(|_| format!("pre-epoch timestamp is not supported: '{timestamp}'"))
}

#[cfg(test)]
mod tests {
    use super::parse_timestamp_to_unix_milliseconds;

    #[test]
    fn parses_supported_timestamp_formats() {
        let cases = [
            ("1970-01-01T00:00:00Z", 0),
            ("1970-01-01T00:00:00.123Z", 123),
            ("1970-01-01T01:00:00+01:00", 0),
            ("1970-01-01T01:00:00.123+01:00", 123),
            ("1970-01-01T01:00:00+0100", 0),
            ("1970-01-01 00:00:00", 0),
            ("1970-01-01 00:00:00.123", 123),
            ("1970-01-01T00:00:00", 0),
            ("1970-01-01T00:00:00.123", 123),
            ("Thu, 01 Jan 1970 00:00:00 GMT", 0),
            ("  1970-01-01T00:00:01Z  ", 1_000),
        ];

        for (timestamp, expected) in cases {
            assert_eq!(
                parse_timestamp_to_unix_milliseconds(timestamp),
                Ok(expected),
                "failed to parse {timestamp}"
            );
        }
    }

    #[test]
    fn rejects_invalid_and_pre_epoch_timestamps() {
        assert!(parse_timestamp_to_unix_milliseconds("").is_err());
        assert!(parse_timestamp_to_unix_milliseconds("not-a-timestamp").is_err());
        assert!(parse_timestamp_to_unix_milliseconds("1969-12-31T23:59:59Z").is_err());
    }

    #[test]
    fn rejects_rfc2822_variants_outside_the_supported_format() {
        let unsupported = [
            "01 Jan 1970 00:00:00 GMT",
            "Thu, 01 Jan 70 00:00:00 GMT",
            "Thu, 01 Jan 1970 00:00:00 +0000",
            "Thu, 01 Jan 1970 00:00:00 UTC",
        ];

        for timestamp in unsupported {
            assert!(
                parse_timestamp_to_unix_milliseconds(timestamp).is_err(),
                "unexpectedly parsed {timestamp}"
            );
        }
    }
}
