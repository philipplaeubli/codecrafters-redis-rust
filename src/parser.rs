type ErrorKind = String;

#[derive(Debug, PartialEq)]
pub enum RedisType {
    SimpleString(String),
    BulkString(String),
    SimpleError(ErrorKind, String),
    Array(Vec<RedisType>),
    None,
}
#[derive(Debug, PartialEq)]
pub enum RespParseError {
    InvalidFormat,
    KeyNotFound,
}

const CRLF: &[u8] = b"\r\n";

pub fn parse_resp(input: &[u8]) -> Result<RedisType, RespParseError> {
    // resp inputs are by definition arrays
    parse_array(input)
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

fn parse_array(input: &[u8]) -> Result<RedisType, RespParseError> {
    let array_with_size_prefix = &input[1..];
    let array_len_delimiter_pos = array_with_size_prefix
        .windows(2)
        .position(|w| w == CRLF)
        .ok_or(RespParseError::InvalidFormat)?;

    let size_as_string = &array_with_size_prefix[0..array_len_delimiter_pos];
    let array_length = str::from_utf8(size_as_string)?.parse::<usize>()?;
    let array_start_position = array_len_delimiter_pos + 2;

    let array = &array_with_size_prefix[array_start_position..];
    let mut elements: Vec<RedisType> = Vec::with_capacity(array_length);
    let mut pos: usize = 0;
    for &byte in array {
        let element = match byte {
            b'+' => parse_simple_string(&array[pos..]),
            b'-' => parse_simple_error(&array[pos..]),
            b'$' => parse_bulk_string(&array[pos..]),
            b'*' => parse_array(&array[pos..]),
            _ => Ok(RedisType::None),
        };

        if let Ok(element) = element
            && element != RedisType::None
        {
            elements.push(element);
        }
        pos += 1;
    }

    Ok(RedisType::Array(elements))
}

fn parse_bulk_string(input: &[u8]) -> Result<RedisType, RespParseError> {
    let bulk_string = &input[1..input.len()];
    // determine bulk string length:
    let str_size_delimiter_pos = bulk_string
        .windows(2)
        .position(|w| w == CRLF)
        .ok_or(RespParseError::InvalidFormat)?;

    let size_as_string = &bulk_string[0..str_size_delimiter_pos];
    let size = str::from_utf8(size_as_string)?.parse::<usize>()?; // this is so neat in rust -> implement the traits, get happy path for free -> no manual error mapping
    let string_start_position = str_size_delimiter_pos + 2;

    let delimiter = &bulk_string[str_size_delimiter_pos..string_start_position];

    // before the actual data, we have a crlf delimiter
    if delimiter != CRLF {
        eprintln!("Invalid format: Expected CRLF delimiter");
        return Err(RespParseError::InvalidFormat);
    }
    let string_end = bulk_string[string_start_position..]
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

    // the actual string starts after the delimiter, goes (starting after the delimiter) until the size.
    let actual_string = &bulk_string[string_start_position..string_start_position + size];

    Ok(RedisType::BulkString(
        String::from_utf8_lossy(actual_string).into(),
    ))
}

fn parse_simple_string(input: &[u8]) -> Result<RedisType, RespParseError> {
    let simple_string = &input[1..];

    // don't parse the whole buffer, but only until the crlf
    let end = simple_string.windows(2).position(|word| word == CRLF);

    if let Some(end) = end {
        // a simple string must not contain \r or \n
        for &byte in &simple_string[..end] {
            if byte == b'\r' || byte == b'\n' {
                eprintln!("Invalid format: Simple string contains invalid characters");
                return Err(RespParseError::InvalidFormat);
            }
        }
        Ok(RedisType::SimpleString(
            String::from_utf8_lossy(&simple_string[..end]).into(),
        ))
    } else {
        Err(RespParseError::InvalidFormat)
    }
}

fn parse_simple_error(input: &[u8]) -> Result<RedisType, RespParseError> {
    let mut simple_error = &input[1..];
    // don't parse the whole buffer, but only until the crlf
    let end = simple_error.windows(2).position(|word| word == CRLF);

    if let Some(end) = end {
        simple_error = &simple_error[..end];
        let mut error_kind = "GENERIC".to_string();
        // walk through the simple error until the first space, if all chars of the first word are uppercase , we set the error_kind to that valute and reutrn the rest as error message
        let mut error_kind_index = 0;
        let mut space_separated = false;
        for &byte in simple_error {
            if byte == b' ' {
                // "word" is finished
                space_separated = true;
                break;
            }
            if !byte.is_ascii_uppercase() {
                break;
            }
            error_kind_index += 1;
        }

        // to have a custom error kind we have only upper case chars in the first word. And the first word must be more than 1 character long (Prevents an error kind of "A error" -> error kind "A")
        if error_kind_index > 1 && space_separated {
            error_kind = String::from_utf8_lossy(&simple_error[..error_kind_index]).into();
            simple_error = &simple_error[error_kind_index + 1..];
        }

        Ok(RedisType::SimpleError(
            error_kind,
            String::from_utf8_lossy(simple_error).into(),
        ))
    } else {
        Err(RespParseError::InvalidFormat)
    }
}

#[test]
fn test_parse_simple_string() {
    let input = b"+OK\r\n";
    let expected = RedisType::SimpleString("OK".to_string());
    assert_eq!(parse_simple_string(input), Ok(expected));
}

#[test]
fn test_parse_simple_string_missing_crlf() {
    let input = b"+OK";
    let expected = RespParseError::InvalidFormat;
    assert_eq!(parse_simple_string(input), Err(expected));
}
#[test]
fn test_parse_simple_string_invalid_crlf_inside() {
    let input = b"+OK\rBye\r\n";
    let expected = RespParseError::InvalidFormat;
    assert_eq!(parse_simple_string(input), Err(expected));
}

#[test]
fn test_parse_simple_error() {
    let input = b"-Error message\r\n";
    let expected = RedisType::SimpleError("GENERIC".to_string(), "Error message".to_string());
    assert_eq!(parse_simple_error(input), Ok(expected));
}

#[test]
fn test_parse_simple_error_with_error_kind() {
    let input = b"-WRONGTYPE Operation against a key holding the wrong kind of error\r\n";
    let expected = RedisType::SimpleError(
        "WRONGTYPE".to_string(),
        "Operation against a key holding the wrong kind of error".to_string(),
    );
    assert_eq!(parse_simple_error(input), Ok(expected));
}
#[test]
fn test_parse_simple_error_with_without_error_kind_one_uppercase_char_start() {
    let input = b"-A thing is broken\r\n";
    let expected = RedisType::SimpleError("GENERIC".into(), "A thing is broken".into());
    assert_eq!(parse_simple_error(input), Ok(expected));
}

#[test]
fn test_parse_bulk_string() {
    let input = b"$5\r\nhello\r\n";
    let expected = RedisType::BulkString("hello".into());
    assert_eq!(parse_bulk_string(input), Ok(expected));
}
#[test]
fn test_parse_bulk_string_with_missing_delimiters() {
    assert_eq!(
        parse_bulk_string(b"$5\rhello\r\n"),
        Err(RespParseError::InvalidFormat)
    );
    assert_eq!(
        parse_bulk_string(b"$5hello\r\n"),
        Err(RespParseError::InvalidFormat)
    );
    assert_eq!(
        parse_bulk_string(b"$5\nhello\r\n"),
        Err(RespParseError::InvalidFormat)
    );

    assert_eq!(
        parse_bulk_string(b"$5\r\nhello"),
        Err(RespParseError::InvalidFormat)
    );
    assert_eq!(
        parse_bulk_string(b"$5\r\nhello\r"),
        Err(RespParseError::InvalidFormat)
    );
    assert_eq!(
        parse_bulk_string(b"$5\r\nhello\n"),
        Err(RespParseError::InvalidFormat)
    );
}
#[test]
fn test_parse_bulk_string_with_size_mismatch() {
    assert_eq!(
        parse_bulk_string(b"$1000\r\nhello\r\n"),
        Err(RespParseError::InvalidFormat)
    );

    assert_eq!(
        parse_bulk_string(b"$6\r\nhello\r\n"),
        Err(RespParseError::InvalidFormat)
    );

    assert_eq!(
        parse_bulk_string(b"$4\r\nhello\r\n"),
        Err(RespParseError::InvalidFormat)
    );
}
#[test]
fn test_parse_bulk_string_with_invalid_size() {
    assert_eq!(
        parse_bulk_string(b"$-1\r\nhello\r\n"),
        Err(RespParseError::InvalidFormat)
    );
}
#[test]
fn test_parse_bulk_string_with_empty_string() {
    assert_eq!(
        parse_bulk_string(b"$0\r\n\r\n"),
        Ok(RedisType::BulkString("".into()))
    );
}

#[test]
fn test_parse_array_empty_array() {
    assert_eq!(parse_resp(b"*0\r\n"), Ok(RedisType::Array(vec![])));
}

#[test]
fn test_parse_array_large_string_array() {
    // ten hellos
    assert_eq!(parse_resp(b"*10\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n$5\r\nhello\r\n"), Ok(RedisType::Array(vec![
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
        RedisType::BulkString("hello".into()),
    ])));
}
