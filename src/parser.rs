use bytes::{Buf, Bytes, BytesMut};

#[derive(Debug, PartialEq)]
pub enum RedisType {
    SimpleString(Bytes),
    BulkString(Bytes),
    Integer(i128),
    NullBulkString,
    SimpleError(Bytes),
    Array(Option<Vec<RedisType>>),
}
#[derive(Debug, PartialEq)]
pub enum RespParseError {
    InvalidFormat,
}

const CRLF: &[u8] = b"\r\n";

pub fn parse_resp(buffer: &mut BytesMut) -> Result<RedisType, RespParseError> {
    // resp inputs are by definition arrays
    parse_array(buffer)
}

impl RedisType {
    pub fn encode(&self, out: &mut BytesMut) {
        match self {
            RedisType::SimpleString(s) => {
                out.extend_from_slice(b"+");
                out.extend_from_slice(&s);
                out.extend_from_slice(b"\r\n");
            }
            RedisType::SimpleError(msg) => {
                out.extend_from_slice(b"-");
                out.extend_from_slice(&msg);
                out.extend_from_slice(b"\r\n");
            }
            RedisType::Integer(n) => {
                out.extend_from_slice(b":");
                // numbers must be ASCII text in RESP
                out.extend_from_slice(n.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RedisType::BulkString(bytes) => {
                out.extend_from_slice(b"$");
                out.extend_from_slice(bytes.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            RedisType::Array(items) => {
                if let Some(items) = items {
                    out.extend_from_slice(b"*");
                    out.extend_from_slice(items.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    for item in items {
                        item.encode(out);
                    }
                } else {
                    out.extend_from_slice(b"*-1\r\n"); // return a null array https://redis.io/docs/latest/develop/reference/protocol-spec/#null-arrays
                }
            }
            RedisType::NullBulkString => {
                out.extend_from_slice(b"$-1\r\n");
            }
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut out = BytesMut::new();
        self.encode(&mut out);
        out.freeze()
    }
}
// Happy path, if we encounter a Utf8Error, we assume that the input is invalid
impl From<std::str::Utf8Error> for RespParseError {
    fn from(_error: std::str::Utf8Error) -> Self {
        RespParseError::InvalidFormat
    }
}
// Happy path, if we encounter a ParseIntError, we assume that the input is invalid
impl From<std::num::ParseIntError> for RespParseError {
    fn from(_error: std::num::ParseIntError) -> Self {
        RespParseError::InvalidFormat
    }
}

impl From<std::io::Error> for RespParseError {
    fn from(_error: std::io::Error) -> Self {
        RespParseError::InvalidFormat
    }
}

fn parse_array(buffer: &mut BytesMut) -> Result<RedisType, RespParseError> {
    // let array_with_size_prefix = &buffer[1..];
    let array_len_delimiter_pos = buffer
        .windows(2)
        .position(|w| w == CRLF)
        .ok_or(RespParseError::InvalidFormat)?;

    let size_as_string = &buffer[1..array_len_delimiter_pos];
    let array_length = str::from_utf8(size_as_string)?.parse::<usize>()?;
    let array_start_position = array_len_delimiter_pos + 2;

    buffer.advance(array_start_position);

    let mut elements: Vec<RedisType> = Vec::with_capacity(array_length);

    while elements.len() < array_length {
        let element = match buffer[0] {
            b'+' => parse_simple_string(buffer),
            b'-' => parse_simple_error(buffer),
            b'$' => parse_bulk_string(buffer),
            b'*' => parse_array(buffer),
            _ => Ok(RedisType::NullBulkString),
        };

        elements.push(element?);
    }

    Ok(RedisType::Array(Some(elements)))
}

fn parse_bulk_string(buffer: &mut BytesMut) -> Result<RedisType, RespParseError> {
    // determine bulk string length:
    let str_size_delimiter_pos = buffer
        .windows(2)
        .position(|w| w == CRLF)
        .ok_or(RespParseError::InvalidFormat)?;
    let size_as_string = &buffer[1..str_size_delimiter_pos];

    let size = str::from_utf8(size_as_string)?.parse::<usize>()?;
    let string_start_position = str_size_delimiter_pos + 2;

    let delimiter = &buffer[str_size_delimiter_pos..string_start_position];
    // before the actual data, we have a crlf delimiter
    if delimiter != CRLF {
        eprintln!("Invalid format: Expected CRLF delimiter");
        return Err(RespParseError::InvalidFormat);
    }
    let string_end = buffer[string_start_position..]
        .windows(2)
        .position(|w| w == CRLF)
        .ok_or(RespParseError::InvalidFormat)?;

    // actual string size is starting after the delimiter and ends before the next crlf
    if string_end != size {
        eprintln!(
            "Size mismatch: Expected size: {}, Actual size: {}",
            size, string_end
        );
        return Err(RespParseError::InvalidFormat);
    }

    buffer.advance(string_start_position);
    let content = buffer.split_to(string_end).freeze();
    buffer.advance(2); // Skip  CRLF

    Ok(RedisType::BulkString(content))
}

fn parse_simple_string(buffer: &mut BytesMut) -> Result<RedisType, RespParseError> {
    // don't parse the whole buffer, but only until the crlf
    let end = buffer.windows(2).position(|word| word == CRLF);
    if let Some(end) = end {
        // a simple string must not contain \r or \n
        for &byte in &buffer[1..end] {
            if byte == b'\r' || byte == b'\n' {
                eprintln!("Invalid format: Simple string contains invalid characters");
                return Err(RespParseError::InvalidFormat);
            }
        }
        buffer.advance(1);
        let content = buffer.split_to(end - 1).freeze();
        buffer.advance(2); // Skip the CRLF

        Ok(RedisType::SimpleString(content))
    } else {
        Err(RespParseError::InvalidFormat)
    }
}

fn parse_simple_error(input: &mut BytesMut) -> Result<RedisType, RespParseError> {
    parse_simple_string(input).and_then(|string| match string {
        RedisType::SimpleString(content) => Ok(RedisType::SimpleError(content)),
        _ => Err(RespParseError::InvalidFormat),
    })
}

#[test]
fn test_parse_simple_string() {
    let mut input = BytesMut::from("+OK\r\n");
    let expected = RedisType::SimpleString(BytesMut::from("OK").freeze());
    assert_eq!(parse_simple_string(&mut input), Ok(expected));
}

#[test]
fn test_parse_simple_string_missing_crlf() {
    let mut input = BytesMut::from("+OK");
    let expected = RespParseError::InvalidFormat;
    assert_eq!(parse_simple_string(&mut input), Err(expected));
}
#[test]
fn test_parse_simple_string_invalid_crlf_inside() {
    let mut input = BytesMut::from("+OK\rBye\r\n");

    let expected = RespParseError::InvalidFormat;
    assert_eq!(parse_simple_string(&mut input), Err(expected));
}

#[test]
fn test_parse_simple_error() {
    let mut input = BytesMut::from("-Error message\r\n");
    let expected = RedisType::SimpleError(BytesMut::from("Error message").freeze());
    assert_eq!(parse_simple_error(&mut input), Ok(expected));
}

#[test]
fn test_parse_simple_error_with_error_kind() {
    let mut input =
        BytesMut::from("-WRONGTYPE Operation against a key holding the wrong kind of error\r\n");
    let expected = RedisType::SimpleError(
        BytesMut::from("WRONGTYPE Operation against a key holding the wrong kind of error")
            .freeze(),
    );
    assert_eq!(parse_simple_error(&mut input), Ok(expected));
}

#[test]
fn test_parse_bulk_string() {
    let mut input = BytesMut::from("$5\r\nhello\r\n");
    let expected = RedisType::BulkString(BytesMut::from("hello").freeze());
    assert_eq!(parse_bulk_string(&mut input), Ok(expected));
}
#[test]
fn test_parse_bulk_string_with_missing_delimiters() {
    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$5\rhello\r\n")),
        Err(RespParseError::InvalidFormat)
    );
    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$5hello\r\n")),
        Err(RespParseError::InvalidFormat)
    );
    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$5\nhello\r\n")),
        Err(RespParseError::InvalidFormat)
    );

    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$5\r\nhello")),
        Err(RespParseError::InvalidFormat)
    );
    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$5\r\nhello\r")),
        Err(RespParseError::InvalidFormat)
    );
    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$5\r\nhello\n")),
        Err(RespParseError::InvalidFormat)
    );
}
#[test]
fn test_parse_bulk_string_with_size_mismatch() {
    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$1000\r\nhello\r\n")),
        Err(RespParseError::InvalidFormat)
    );

    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$6\r\nhello\r\n")),
        Err(RespParseError::InvalidFormat)
    );

    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$4\r\nhello\r\n")),
        Err(RespParseError::InvalidFormat)
    );
}
#[test]
fn test_parse_bulk_string_with_invalid_size() {
    assert_eq!(
        parse_bulk_string(&mut BytesMut::from("$-1\r\nhello\r\n")),
        Err(RespParseError::InvalidFormat)
    );
}
#[test]
fn test_parse_bulk_string_with_empty_string() {
    let mut input = BytesMut::from("$0\r\n\r\n");
    let res = parse_bulk_string(&mut input).unwrap().to_bytes();
    assert_eq!(res.as_ref(), b"$0\r\n\r\n");
}

#[test]
fn test_parse_lrange_array() {
    let mut input = BytesMut::from("*4\r\n$6\r\nLRANGE\r\n$4\r\npear\r\n$2\r\n-3\r\n$2\r\n-1\r\n");

    assert_eq!(
        parse_array(&mut input),
        Ok(RedisType::Array(Some(vec![
            RedisType::BulkString(BytesMut::from("LRANGE").freeze()),
            RedisType::BulkString(BytesMut::from("pear").freeze()),
            RedisType::BulkString(BytesMut::from("-3").freeze()),
            RedisType::BulkString(BytesMut::from("-1").freeze()),
        ])))
    );
}

#[test]
fn test_parse_array_empty_array() {
    let mut input = BytesMut::from("*0\r\n");
    assert_eq!(parse_array(&mut input), Ok(RedisType::Array(Some(vec![]))));
}

#[test]
fn test_parse_array_null_array() {
    let mut input = BytesMut::from("*-1\r\n");
    assert_eq!(parse_array(&mut input), Ok(RedisType::Array(None)));
}

#[test]
fn test_parse_array_large_string_array() {
    let mut buffer = BytesMut::from(
        "*10\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n",
    );

    assert_eq!(
        parse_array(&mut buffer),
        Ok(RedisType::Array(Some(vec![
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
            RedisType::BulkString(BytesMut::from("hello").freeze()),
        ])))
    )
}
#[test]
fn test_parse_array_nested_array() {
    let mut input =
        BytesMut::from("*3\r\n$3\r\nfoo\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nbar\r\n");

    assert_eq!(
        parse_array(&mut input),
        Ok(RedisType::Array(Some(vec![
            RedisType::BulkString(BytesMut::from("foo").freeze()),
            RedisType::Array(Some(vec![
                RedisType::BulkString(BytesMut::from("hello").freeze()),
                RedisType::BulkString(BytesMut::from("world").freeze()),
            ])),
            RedisType::BulkString(BytesMut::from("bar").freeze()),
        ])))
    );
}
