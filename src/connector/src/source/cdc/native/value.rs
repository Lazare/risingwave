// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Conversion from the Postgres text representation of a value to the JSON representation
//! that the Debezium Postgres connector produces under the configuration used by RisingWave
//! (`decimal.handling.mode=string`, `time.precision.mode=microseconds`,
//! `interval.handling.mode=string`), so that the downstream Debezium JSON parser can be reused
//! as is.
//!
//! The replication connection is expected to run with `DateStyle=ISO`, `TimeZone=UTC` and
//! `IntervalStyle=iso_8601`.

use std::io::Write;

use base64::Engine;
use chrono::{NaiveTime, Timelike};
use risingwave_common::types::DEBEZIUM_UNAVAILABLE_VALUE;

/// OIDs below this value are assigned to built-in types.
const FIRST_NORMAL_OID: u32 = 16384;

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// How to convert the text representation of a Postgres value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgValueKind {
    Bool,
    /// `int2`, `int4`, `int8`, `oid`
    Int,
    /// `float4`, `float8`
    Float,
    Numeric,
    Money,
    /// Emitted as a JSON string as is, e.g. `text`, `uuid`, `jsonb`, enums.
    Text,
    Bytea,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
    Point,
    /// `PostGIS` `geometry` and `geography`
    Geometry,
    /// An array of the given elements, separated by the given delimiter, which is `,` for
    /// all the types but a few (e.g. `:` for the `PostGIS` types).
    Array(Box<PgValueKind>, u8),
}

impl PgValueKind {
    /// An array with the default delimiter.
    pub fn array_of(elem: PgValueKind) -> Self {
        PgValueKind::Array(Box::new(elem), b',')
    }

    pub fn array_with_delimiter(elem: PgValueKind, delimiter: u8) -> Self {
        PgValueKind::Array(Box::new(elem), delimiter)
    }
}

/// Returns the conversion of a built-in type, or `None` for a user-defined type, which
/// has to be resolved from the catalog of the upstream database.
pub fn builtin_value_kind(type_oid: u32) -> Option<PgValueKind> {
    use PgValueKind::*;
    if type_oid >= FIRST_NORMAL_OID {
        return None;
    }
    let kind = match type_oid {
        16 => Bool,
        20 | 21 | 23 | 26 => Int,
        700 | 701 => Float,
        1700 => Numeric,
        790 => Money,
        17 => Bytea,
        1082 => Date,
        1083 => Time,
        1266 => TimeTz,
        1114 => Timestamp,
        1184 => TimestampTz,
        1186 => Interval,
        600 => Point,
        // arrays
        1000 => PgValueKind::array_of(Bool),
        1005 | 1007 | 1016 | 1028 => PgValueKind::array_of(Int),
        1021 | 1022 => PgValueKind::array_of(Float),
        1231 => PgValueKind::array_of(Numeric),
        791 => PgValueKind::array_of(Money),
        1001 => PgValueKind::array_of(Bytea),
        1182 => PgValueKind::array_of(Date),
        1183 => PgValueKind::array_of(Time),
        1270 => PgValueKind::array_of(TimeTz),
        1115 => PgValueKind::array_of(Timestamp),
        1185 => PgValueKind::array_of(TimestampTz),
        1187 => PgValueKind::array_of(Interval),
        1017 => PgValueKind::array_of(Point),
        // _json, _xml, _char, _name, _text, _bpchar, _varchar, _uuid, _jsonb,
        // _cidr, _macaddr, _inet
        199 | 143 | 1002 | 1003 | 1009 | 1014 | 1015 | 2951 | 3807 | 651 | 1040 | 1041 => {
            PgValueKind::array_of(Text)
        }
        // text, varchar, bpchar, name, json, jsonb, uuid, xml, inet and the others
        _ => Text,
    };
    Some(kind)
}

pub fn write_json_string(buf: &mut Vec<u8>, s: &str) {
    serde_json::to_writer(&mut *buf, s).expect("writing a string to a Vec never fails");
}

/// Write the JSON representation of the placeholder of an unchanged TOAST value.
pub fn write_unavailable_value(buf: &mut Vec<u8>, kind: &PgValueKind) {
    match kind {
        PgValueKind::Bytea => {
            write_json_string(
                buf,
                &base64::engine::general_purpose::STANDARD.encode(DEBEZIUM_UNAVAILABLE_VALUE),
            );
        }
        PgValueKind::Array(elem, _) => {
            buf.push(b'[');
            if matches!(**elem, PgValueKind::Int) {
                // Debezium represents the placeholder of an integer array by the bytes of the
                // placeholder string.
                for (i, b) in DEBEZIUM_UNAVAILABLE_VALUE.bytes().enumerate() {
                    if i > 0 {
                        buf.push(b',');
                    }
                    write!(buf, "{}", b).unwrap();
                }
            } else {
                write_unavailable_value(buf, elem);
            }
            buf.push(b']');
        }
        _ => write_json_string(buf, DEBEZIUM_UNAVAILABLE_VALUE),
    }
}

/// Write the JSON representation of a non-null value given in the Postgres text format.
///
/// A value that cannot be interpreted as its type is written as a JSON string as is, and it
/// is up to the downstream parser to accept or reject it.
pub fn write_value(buf: &mut Vec<u8>, kind: &PgValueKind, text: &str) {
    let written = match kind {
        PgValueKind::Bool => match text {
            "t" => {
                buf.extend_from_slice(b"true");
                true
            }
            "f" => {
                buf.extend_from_slice(b"false");
                true
            }
            _ => false,
        },
        PgValueKind::Int => match text.parse::<i64>() {
            Ok(v) => {
                write!(buf, "{}", v).unwrap();
                true
            }
            Err(_) => false,
        },
        PgValueKind::Float => write_float(buf, text),
        PgValueKind::Numeric => {
            let s = match text {
                "NaN" => "NAN",
                "Infinity" => "POSITIVE_INFINITY",
                "-Infinity" => "NEGATIVE_INFINITY",
                other => other,
            };
            write_json_string(buf, s);
            true
        }
        PgValueKind::Money => {
            let negative = text.contains('-') || text.contains('(');
            let mut s = String::with_capacity(text.len());
            if negative {
                s.push('-');
            }
            s.extend(text.chars().filter(|c| c.is_ascii_digit() || *c == '.'));
            write_json_string(buf, &s);
            true
        }
        PgValueKind::Text => {
            write_json_string(buf, text);
            true
        }
        PgValueKind::Bytea => match text.strip_prefix("\\x").and_then(decode_hex) {
            Some(bytes) => {
                write_json_string(
                    buf,
                    &base64::engine::general_purpose::STANDARD.encode(bytes),
                );
                true
            }
            None => false,
        },
        PgValueKind::Date => match parse_date_days(text) {
            Some(days) => {
                write!(buf, "{}", days).unwrap();
                true
            }
            None => false,
        },
        PgValueKind::Time => match parse_time_micros(text) {
            Some(micros) => {
                write!(buf, "{}", micros).unwrap();
                true
            }
            None => false,
        },
        PgValueKind::TimeTz => match split_utc_offset(text, 0) {
            Some((time, offset_secs)) => match parse_time_micros(time) {
                Some(micros) => {
                    let utc = (micros - offset_secs * 1_000_000).rem_euclid(MICROS_PER_DAY);
                    let time = NaiveTime::from_num_seconds_from_midnight_opt(
                        (utc / 1_000_000) as u32,
                        ((utc % 1_000_000) * 1000) as u32,
                    )
                    .unwrap();
                    write_json_string(buf, &format!("{}Z", time.format("%H:%M:%S%.f")));
                    true
                }
                None => false,
            },
            None => false,
        },
        PgValueKind::Timestamp => match parse_timestamp_micros(text) {
            Some(micros) => {
                write!(buf, "{}", micros).unwrap();
                true
            }
            None => false,
        },
        PgValueKind::TimestampTz => match parse_timestamptz_micros(text) {
            Some(micros) => {
                write_json_string(buf, &format_utc_timestamp(micros));
                true
            }
            None => false,
        },
        PgValueKind::Interval => match normalize_interval(text) {
            Some(interval) => {
                write_json_string(buf, &interval);
                true
            }
            None => false,
        },
        PgValueKind::Point => write_point(buf, text),
        PgValueKind::Geometry => match decode_hex(text) {
            Some(bytes) => {
                buf.extend_from_slice(b"{\"srid\":");
                write!(buf, "{}", ewkb_srid(&bytes).unwrap_or(0)).unwrap();
                buf.extend_from_slice(b",\"wkb\":");
                write_json_string(
                    buf,
                    &base64::engine::general_purpose::STANDARD.encode(bytes),
                );
                buf.push(b'}');
                true
            }
            None => false,
        },
        PgValueKind::Array(elem, delimiter) => match parse_array(text, *delimiter) {
            Some(node) => {
                write_array_node(buf, elem, &node);
                true
            }
            None => false,
        },
    };
    if !written {
        write_json_string(buf, text);
    }
}

fn write_float(buf: &mut Vec<u8>, text: &str) -> bool {
    match text {
        // JSON has no representation for the non-finite numbers.
        "NaN" | "Infinity" | "-Infinity" => {
            write_json_string(buf, text);
            true
        }
        _ => match text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
        {
            Some(v) => {
                write!(buf, "{}", v).unwrap();
                true
            }
            None => false,
        },
    }
}

/// `(x,y)` to `{"x":x,"y":y}`
fn write_point(buf: &mut Vec<u8>, text: &str) -> bool {
    let Some((x, y)) = text
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .and_then(|s| s.split_once(','))
    else {
        return false;
    };
    let mut tmp = Vec::new();
    tmp.extend_from_slice(b"{\"x\":");
    if !write_float(&mut tmp, x) {
        return false;
    }
    tmp.extend_from_slice(b",\"y\":");
    if !write_float(&mut tmp, y) {
        return false;
    }
    tmp.push(b'}');
    buf.extend_from_slice(&tmp);
    true
}

fn parse_time_micros(text: &str) -> Option<i64> {
    if text == "24:00:00" {
        return Some(MICROS_PER_DAY);
    }
    let time = NaiveTime::parse_from_str(text, "%H:%M:%S%.f").ok()?;
    Some(time.num_seconds_from_midnight() as i64 * 1_000_000 + (time.nanosecond() / 1000) as i64)
}

/// Days since the Unix epoch of a date of the proleptic Gregorian calendar, where the year 0
/// is 1 BC.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The inverse of [`days_from_civil`], returns `(year, month, day)`.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = year_of_era + era * 400 + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

/// Strip the ` BC` suffix of a date or timestamp.
fn strip_bc(text: &str) -> (&str, bool) {
    match text.strip_suffix(" BC") {
        Some(text) => (text, true),
        None => (text, false),
    }
}

/// Parse `<year>-<month>-<day>` into the days since the Unix epoch. The year can have more
/// than 4 digits, and `bc` tells that it is before Christ, which `chrono` does not parse.
fn parse_civil_date(text: &str, bc: bool) -> Option<i64> {
    let mut parts = text.split('-');
    let year = parts.next()?.parse::<u32>().ok()? as i64;
    let month = parts.next()?.parse::<u32>().ok()? as i64;
    let day = parts.next()?.parse::<u32>().ok()? as i64;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // 1 BC is the year 0, 2 BC is the year -1, ...
    let year = if bc { 1 - year } else { year };
    Some(days_from_civil(year, month, day))
}

fn parse_date_days(text: &str) -> Option<i64> {
    let (text, bc) = strip_bc(text);
    parse_civil_date(text, bc)
}

/// Parse `<date> <time>` into the microseconds since the Unix epoch.
fn parse_civil_timestamp(text: &str, bc: bool) -> Option<i64> {
    let (date, time) = text.split_once(' ')?;
    parse_civil_date(date, bc)?
        .checked_mul(MICROS_PER_DAY)?
        .checked_add(parse_time_micros(time)?)
}

fn parse_timestamp_micros(text: &str) -> Option<i64> {
    let (text, bc) = strip_bc(text);
    parse_civil_timestamp(text, bc)
}

/// Parse `<date> <time><offset>` into the microseconds since the Unix epoch.
fn parse_timestamptz_micros(text: &str) -> Option<i64> {
    let (text, bc) = strip_bc(text);
    // The date part contains `-`, so only look for the offset after it.
    let (timestamp, offset_secs) = split_utc_offset(text, text.find(' ')?)?;
    parse_civil_timestamp(timestamp, bc)?.checked_sub(offset_secs.checked_mul(1_000_000)?)
}

/// Format the microseconds since the Unix epoch as an ISO 8601 timestamp in UTC, in the same
/// way as Java does: years beyond 9999 are prefixed by `+`, and years before 1 AD are
/// written as negative proleptic years.
fn format_utc_timestamp(micros: i64) -> String {
    let (year, month, day) = civil_from_days(micros.div_euclid(MICROS_PER_DAY));
    let micros_of_day = micros.rem_euclid(MICROS_PER_DAY);
    let seconds = micros_of_day / 1_000_000;
    let fraction = micros_of_day % 1_000_000;
    let year = if year > 9999 {
        format!("+{}", year)
    } else if year < 0 {
        format!("-{:04}", -year)
    } else {
        format!("{:04}", year)
    };
    let fraction = if fraction == 0 {
        String::new()
    } else if fraction % 1000 == 0 {
        format!(".{:03}", fraction / 1000)
    } else {
        format!(".{:06}", fraction)
    };
    format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}{}Z",
        year,
        month,
        day,
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60,
        fraction
    )
}

/// Rewrite an interval in the `iso_8601` style of Postgres, which omits the zero components
/// (e.g. `P1DT2H`, `PT0S`), with all the components, as Debezium does: `P0Y0M1DT2H0M0S`.
fn normalize_interval(text: &str) -> Option<String> {
    let rest = text.strip_prefix('P')?;
    let (date, time) = rest.split_once('T').unwrap_or((rest, ""));
    // years, months, days, hours, minutes, seconds
    let mut components = ["0"; 6];
    for (section, designators, base) in [(date, "YMD", 0), (time, "HMS", 3)] {
        let mut start = 0;
        for (i, c) in section.char_indices() {
            if c.is_ascii_digit() || c == '-' || c == '.' {
                continue;
            }
            let value = &section[start..i];
            if value.is_empty() || value.parse::<f64>().is_err() {
                return None;
            }
            components[base + designators.find(c)?] = value;
            start = i + 1;
        }
        if start != section.len() {
            return None;
        }
    }
    let [years, months, days, hours, minutes, seconds] = components;
    Some(format!(
        "P{}Y{}M{}DT{}H{}M{}S",
        years, months, days, hours, minutes, seconds
    ))
}

/// Split `<value><+|-><hh>[:<mm>[:<ss>]]` into the value and the offset in seconds.
/// The sign of the offset is searched from the byte position `from`.
fn split_utc_offset(text: &str, from: usize) -> Option<(&str, i64)> {
    let pos = from + text.get(from..)?.rfind(['+', '-'])?;
    let (value, offset) = text.split_at(pos);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let mut secs = 0i64;
    let mut unit = 3600;
    for part in offset[1..].split(':') {
        if unit == 0 || part.len() != 2 {
            return None;
        }
        secs += part.parse::<u8>().ok()? as i64 * unit;
        unit /= 60;
    }
    Some((value, sign * secs))
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let nibble = |b: u8| -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    };
    bytes
        .chunks_exact(2)
        .map(|pair| Some((nibble(pair[0])? << 4) | nibble(pair[1])?))
        .collect()
}

/// Extract the SRID of an EWKB geometry, if present.
fn ewkb_srid(bytes: &[u8]) -> Option<u32> {
    const SRID_FLAG: u32 = 0x2000_0000;
    let little_endian = *bytes.first()? == 1;
    let read = |range: std::ops::Range<usize>| -> Option<u32> {
        let raw: [u8; 4] = bytes.get(range)?.try_into().ok()?;
        Some(if little_endian {
            u32::from_le_bytes(raw)
        } else {
            u32::from_be_bytes(raw)
        })
    };
    let type_code = read(1..5)?;
    if type_code & SRID_FLAG != 0 {
        read(5..9)
    } else {
        None
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ArrayNode {
    Null,
    Value(String),
    Array(Vec<ArrayNode>),
}

/// Parse a Postgres array literal, e.g. `{1,NULL,"a b",{2,3}}`.
fn parse_array(text: &str, delimiter: u8) -> Option<ArrayNode> {
    let bytes = text.as_bytes();
    // Skip the optional dimension decoration, e.g. `[0:2]={1,2,3}`.
    let mut pos = if bytes.first() == Some(&b'[') {
        text.find('=')? + 1
    } else {
        0
    };
    let node = parse_array_at(bytes, &mut pos, 0, delimiter)?;
    (pos == bytes.len()).then_some(node)
}

fn parse_array_at(bytes: &[u8], pos: &mut usize, depth: usize, delimiter: u8) -> Option<ArrayNode> {
    // Postgres supports at most 6 dimensions.
    if depth > 6 || *bytes.get(*pos)? != b'{' {
        return None;
    }
    *pos += 1;
    let mut items = vec![];
    if *bytes.get(*pos)? == b'}' {
        *pos += 1;
        return Some(ArrayNode::Array(items));
    }
    loop {
        match *bytes.get(*pos)? {
            b'{' => items.push(parse_array_at(bytes, pos, depth + 1, delimiter)?),
            b'"' => {
                *pos += 1;
                let mut value = vec![];
                loop {
                    match *bytes.get(*pos)? {
                        b'"' => break,
                        b'\\' => {
                            *pos += 1;
                            value.push(*bytes.get(*pos)?);
                        }
                        b => value.push(b),
                    }
                    *pos += 1;
                }
                *pos += 1;
                items.push(ArrayNode::Value(String::from_utf8(value).ok()?));
            }
            _ => {
                let start = *pos;
                loop {
                    let b = *bytes.get(*pos)?;
                    if b == delimiter || b == b'}' {
                        break;
                    }
                    *pos += 1;
                }
                let value = std::str::from_utf8(&bytes[start..*pos]).ok()?;
                if value.eq_ignore_ascii_case("NULL") {
                    items.push(ArrayNode::Null);
                } else {
                    items.push(ArrayNode::Value(value.to_owned()));
                }
            }
        }
        match *bytes.get(*pos)? {
            b'}' => {
                *pos += 1;
                return Some(ArrayNode::Array(items));
            }
            b if b == delimiter => *pos += 1,
            _ => return None,
        }
    }
}

fn write_array_node(buf: &mut Vec<u8>, elem: &PgValueKind, node: &ArrayNode) {
    match node {
        ArrayNode::Null => buf.extend_from_slice(b"null"),
        // Debezium wraps the end of the day around in the arrays of `time`, while it is not
        // representable as a scalar.
        ArrayNode::Value(v) if *elem == PgValueKind::Time && v == "24:00:00" => buf.push(b'0'),
        ArrayNode::Value(v) => write_value(buf, elem, v),
        ArrayNode::Array(items) => {
            buf.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                write_array_node(buf, elem, item);
            }
            buf.push(b']');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_json(kind: &PgValueKind, text: &str) -> String {
        let mut buf = vec![];
        write_value(&mut buf, kind, text);
        String::from_utf8(buf).unwrap()
    }

    fn builtin(oid: u32, text: &str) -> String {
        to_json(&builtin_value_kind(oid).unwrap(), text)
    }

    #[test]
    fn test_scalars() {
        assert_eq!(builtin(16, "t"), "true");
        assert_eq!(builtin(16, "f"), "false");
        assert_eq!(builtin(23, "-42"), "-42");
        assert_eq!(builtin(20, "9223372036854775807"), "9223372036854775807");
        assert_eq!(builtin(701, "1.5"), "1.5");
        assert_eq!(builtin(700, "1.1"), "1.1");
        assert_eq!(builtin(701, "1e+20"), "1e20");
        assert_eq!(builtin(701, "NaN"), "\"NaN\"");
        assert_eq!(builtin(1700, "50.50"), "\"50.50\"");
        assert_eq!(builtin(1700, "NaN"), "\"NAN\"");
        assert_eq!(builtin(1700, "-Infinity"), "\"NEGATIVE_INFINITY\"");
        assert_eq!(builtin(790, "-$1,234.56"), "\"-1234.56\"");
        assert_eq!(builtin(25, "a \"quoted\"\nline"), r#""a \"quoted\"\nline""#);
        assert_eq!(builtin(3802, r#"{"a": 1}"#), r#""{\"a\": 1}""#);
        assert_eq!(builtin(17, "\\x68656c6c6f"), "\"aGVsbG8=\"");
        assert_eq!(builtin(1186, "P1Y2M3DT4H5M6S"), "\"P1Y2M3DT4H5M6S\"");
        assert_eq!(builtin(1186, "PT0S"), "\"P0Y0M0DT0H0M0S\"");
        assert_eq!(builtin(1186, "PT100H"), "\"P0Y0M0DT100H0M0S\"");
        assert_eq!(builtin(1186, "P1DT0.000001S"), "\"P0Y0M1DT0H0M0.000001S\"");
        assert_eq!(builtin(1186, "P1M-1D"), "\"P0Y1M-1DT0H0M0S\"");
        assert_eq!(builtin(1186, "P-1DT-2H-0.5S"), "\"P0Y0M-1DT-2H0M-0.5S\"");
        assert_eq!(builtin(1186, "1 day"), "\"1 day\"");
        // unknown built-in types are passed through
        assert_eq!(builtin(869, "192.168.0.1"), "\"192.168.0.1\"");
        assert_eq!(builtin_value_kind(16384), None);
    }

    #[test]
    fn test_datetime() {
        assert_eq!(builtin(1082, "1970-01-02"), "1");
        assert_eq!(builtin(1082, "2024-01-30"), "19752");
        assert_eq!(builtin(1082, "1969-12-31"), "-1");
        assert_eq!(builtin(1082, "infinity"), "\"infinity\"");
        assert_eq!(builtin(1083, "00:00:01.5"), "1500000");
        assert_eq!(builtin(1083, "24:00:00"), "86400000000");
        assert_eq!(builtin(1114, "1970-01-01 00:00:01.000002"), "1000002");
        assert_eq!(builtin(1114, "2024-01-30 12:34:56"), "1706618096000000");
        assert_eq!(
            builtin(1184, "2024-04-11 02:00:00.123456+00"),
            "\"2024-04-11T02:00:00.123456Z\""
        );
        assert_eq!(
            builtin(1184, "2024-04-11 02:00:00+00"),
            "\"2024-04-11T02:00:00Z\""
        );
        assert_eq!(
            builtin(1184, "2024-04-11 02:00:00+05:30"),
            "\"2024-04-10T20:30:00Z\""
        );
        assert_eq!(
            builtin(1184, "2024-04-11 02:00:00-08"),
            "\"2024-04-11T10:00:00Z\""
        );
        assert_eq!(builtin(1184, "infinity"), "\"infinity\"");
        // before Christ and beyond the year 9999
        assert_eq!(builtin(1082, "0001-01-01"), "-719162");
        assert_eq!(builtin(1082, "0001-12-31 BC"), "-719163");
        assert_eq!(builtin(1082, "0044-03-15 BC"), "-735160");
        assert_eq!(builtin(1082, "10000-01-01"), "2932897");
        assert_eq!(builtin(1114, "10000-01-01 00:00:00"), "253402300800000000");
        assert_eq!(
            builtin(1114, "0001-12-31 23:59:59.5 BC"),
            "-62135596800500000"
        );
        assert_eq!(builtin(1114, "infinity"), "\"infinity\"");
        assert_eq!(
            builtin(1184, "10000-01-01 00:00:00+00"),
            "\"+10000-01-01T00:00:00Z\""
        );
        assert_eq!(
            builtin(1184, "0044-03-15 12:00:00+00 BC"),
            "\"-0043-03-15T12:00:00Z\""
        );
        assert_eq!(
            builtin(1184, "0001-01-01 00:00:00.5+00"),
            "\"0001-01-01T00:00:00.500Z\""
        );
        assert_eq!(builtin(1266, "04:05:06.789-08"), "\"12:05:06.789Z\"");
        assert_eq!(builtin(1266, "01:00:00+02"), "\"23:00:00Z\"");
    }

    #[test]
    fn test_arrays() {
        assert_eq!(builtin(1007, "{1,2,NULL}"), "[1,2,null]");
        assert_eq!(builtin(1007, "{}"), "[]");
        assert_eq!(builtin(1007, "{{1,2},{3,4}}"), "[[1,2],[3,4]]");
        assert_eq!(builtin(1007, "[0:1]={1,2}"), "[1,2]");
        assert_eq!(
            builtin(1009, r#"{a,"b c","d\"e","NULL",NULL,"x\\y"}"#),
            r#"["a","b c","d\"e","NULL",null,"x\\y"]"#
        );
        assert_eq!(builtin(1231, "{1.10,NaN}"), r#"["1.10","NAN"]"#);
        assert_eq!(builtin(1182, "{1970-01-02}"), "[1]");
        assert_eq!(builtin(1183, "{00:00:01,24:00:00}"), "[1000000,0]");
        assert_eq!(
            builtin(1187, "{P1D,PT0S}"),
            r#"["P0Y0M1DT0H0M0S","P0Y0M0DT0H0M0S"]"#
        );
        assert_eq!(
            builtin(1185, r#"{"2024-04-11 02:00:00+00"}"#),
            r#"["2024-04-11T02:00:00Z"]"#
        );
        // the delimiter depends on the element type
        let geometries = PgValueKind::array_with_delimiter(PgValueKind::Text, b':');
        assert_eq!(
            to_json(&geometries, "{0101:0102,03:NULL}"),
            r#"["0101","0102,03",null]"#
        );
        // malformed literals are passed through
        assert_eq!(builtin(1007, "{1,2"), "\"{1,2\"");
    }

    #[test]
    fn test_point_and_geometry() {
        assert_eq!(builtin(600, "(1.5,-2)"), r#"{"x":1.5,"y":-2.0}"#);
        // SRID=4326;POINT(1 2)
        let ewkb = "0101000020E6100000000000000000F03F0000000000000040";
        let json = to_json(&PgValueKind::Geometry, ewkb);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["srid"], 4326);
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(value["wkb"].as_str().unwrap())
                .unwrap(),
            decode_hex(ewkb).unwrap()
        );
    }

    #[test]
    fn test_unavailable_value() {
        let render = |kind: &PgValueKind| {
            let mut buf = vec![];
            write_unavailable_value(&mut buf, kind);
            String::from_utf8(buf).unwrap()
        };
        assert_eq!(
            render(&PgValueKind::Text),
            "\"__debezium_unavailable_value\""
        );
        assert_eq!(
            render(&PgValueKind::Bytea),
            "\"X19kZWJleml1bV91bmF2YWlsYWJsZV92YWx1ZQ==\""
        );
        assert_eq!(
            render(&PgValueKind::array_of(PgValueKind::Text)),
            "[\"__debezium_unavailable_value\"]"
        );
        assert!(render(&PgValueKind::array_of(PgValueKind::Int)).starts_with("[95,95,100,"));
    }
}
