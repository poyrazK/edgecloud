//! RESP2 protocol parser (decode-only).
//!
//! Extracted from `samples/redis-lite/src/resp.rs` into its own crate
//! so the unit tests can run on the host (the parent crate is
//! `#![no_main]` for the WASM cdylib, which prevents `cargo test`
//! from linking a test binary). There is no second copy in this crate;
//! edit the parser here and the parent picks up the new logic.
//!
//! Handles: simple strings (`+OK\r\n`), errors (`-...\r\n`), integers
//! (`:N\r\n`), bulk strings (`$N\r\n<bytes>\r\n` or `$-1\r\n` for nil),
//! and arrays (`*N\r\n <frame> <frame> ...`).
//!
//! No-std-friendly: only depends on `core::` types plus the standard
//! library's `Vec`/`String`/`format!`. Compiles on
//! `wasm32-unknown-unknown`.

#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    /// `Some(bytes)` for a bulk string, `None` for nil.
    Bulk(Option<Vec<u8>>),
    Array(Vec<Frame>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Not enough bytes yet — caller should read more and try again.
    Incomplete,
    /// Bytes don't decode as RESP. The connection is desynced; caller
    /// should send `-ERR ...` and close.
    BadProtocol,
}

/// Parse exactly one RESP frame off the front of `input`, returning the
/// frame plus the unconsumed remainder.
pub fn parse(input: &[u8]) -> Result<(Frame, &[u8]), Error> {
    if input.is_empty() {
        return Err(Error::Incomplete);
    }
    match input[0] {
        b'+' => parse_line(&input[1..]).map(|(s, rest)| (Frame::Simple(s), rest)),
        b'-' => parse_line(&input[1..]).map(|(s, rest)| (Frame::Error(s), rest)),
        b':' => parse_line(&input[1..]).and_then(|(s, rest)| {
            let n: i64 = s.parse().map_err(|_| Error::BadProtocol)?;
            Ok((Frame::Integer(n), rest))
        }),
        b'$' => parse_bulk(&input[1..]),
        b'*' => parse_array(&input[1..]),
        _ => Err(Error::BadProtocol),
    }
}

fn parse_line(input: &[u8]) -> Result<(String, &[u8]), Error> {
    let end = match input.windows(2).position(|w| w == b"\r\n") {
        Some(i) => i,
        None => {
            if input.contains(&b'\n') {
                return Err(Error::BadProtocol);
            }
            return Err(Error::Incomplete);
        }
    };
    let line = std::str::from_utf8(&input[..end])
        .map_err(|_| Error::BadProtocol)?
        .to_string();
    Ok((line, &input[end + 2..]))
}

fn parse_bulk(input: &[u8]) -> Result<(Frame, &[u8]), Error> {
    let (header, rest) = parse_line(input)?;
    let len: i64 = header.parse().map_err(|_| Error::BadProtocol)?;
    if len == -1 {
        return Ok((Frame::Bulk(None), rest));
    }
    if len < 0 {
        return Err(Error::BadProtocol);
    }
    let len = len as usize;
    if rest.len() < len + 2 {
        return Err(Error::Incomplete);
    }
    if &rest[len..len + 2] != b"\r\n" {
        return Err(Error::BadProtocol);
    }
    let bytes = rest[..len].to_vec();
    Ok((Frame::Bulk(Some(bytes)), &rest[len + 2..]))
}

fn parse_array(input: &[u8]) -> Result<(Frame, &[u8]), Error> {
    let (header, mut rest) = parse_line(input)?;
    let count: i64 = header.parse().map_err(|_| Error::BadProtocol)?;
    if count < 0 {
        return Err(Error::BadProtocol);
    }
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (frame, r) = parse(rest)?;
        items.push(frame);
        rest = r;
    }
    Ok((Frame::Array(items), rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ping() {
        let (frame, rest) = parse(b"*1\r\n$4\r\nPING\r\n").unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::Bulk(Some(b"PING".to_vec()))])
        );
        assert!(rest.is_empty());
    }

    #[test]
    fn parse_simple_ok() {
        let (frame, rest) = parse(b"+OK\r\n").unwrap();
        assert_eq!(frame, Frame::Simple("OK".to_string()));
        assert!(rest.is_empty());
    }

    #[test]
    fn parse_integer_one() {
        let (frame, rest) = parse(b":1\r\n").unwrap();
        assert_eq!(frame, Frame::Integer(1));
        assert!(rest.is_empty());
    }

    #[test]
    fn parse_set_then_get_round_trip() {
        let bytes = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (frame, rest) = parse(bytes).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Bulk(Some(b"SET".to_vec())),
                Frame::Bulk(Some(b"foo".to_vec())),
                Frame::Bulk(Some(b"bar".to_vec())),
            ])
        );
        assert!(rest.is_empty());

        let bytes = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let (frame, rest) = parse(bytes).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Bulk(Some(b"GET".to_vec())),
                Frame::Bulk(Some(b"foo".to_vec())),
            ])
        );
        assert!(rest.is_empty());
    }

    #[test]
    fn parse_nil_bulk() {
        let (frame, rest) = parse(b"$-1\r\n").unwrap();
        assert_eq!(frame, Frame::Bulk(None));
        assert!(rest.is_empty());
    }

    #[test]
    fn parse_array_of_three_with_remainder() {
        let bytes = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let (frame, rest) = parse(bytes).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::Bulk(Some(b"PING".to_vec()))])
        );
        let (frame2, rest2) = parse(rest).unwrap();
        assert_eq!(
            frame2,
            Frame::Array(vec![Frame::Bulk(Some(b"PING".to_vec()))])
        );
        assert!(rest2.is_empty());
    }

    #[test]
    fn parse_incomplete_returns_incomplete() {
        assert_eq!(parse(b"$3\r\nfoo"), Err(Error::Incomplete));
        assert_eq!(parse(b"*2\r\n"), Err(Error::Incomplete));
        let bytes = b"*2\r\n$3\r\nGET\r\n$3";
        let err = parse(bytes).unwrap_err();
        assert_eq!(err, Error::Incomplete);
    }

    #[test]
    fn parse_bad_protocol() {
        assert_eq!(parse(b"?\r\n"), Err(Error::BadProtocol));
        assert_eq!(parse(b"$3\r\nfo"), Err(Error::Incomplete));
        assert_eq!(parse(b"$3\r\nfooXX"), Err(Error::BadProtocol));
        assert_eq!(parse(b"$-2\r\n"), Err(Error::BadProtocol));
        // Lone LF (no CR) is rejected as BadProtocol — `parse_line`
        // returns Incomplete when no \r\n terminator is seen, then
        // escalates to BadProtocol when a bare \n is present.
        assert_eq!(parse(b"+OK\n"), Err(Error::BadProtocol));
        assert_eq!(parse(b"-ERR oops\n"), Err(Error::BadProtocol));
    }
}