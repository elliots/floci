//! The gRPC health answer: 87 bytes with no HPACK state.
//!
//! Kubelet's gRPC probe is grpc-go over h2c: a connection preface, `SETTINGS`, one
//! `Health/Check` call on stream 1, and `GOAWAY` when it is done. grpc-go sends no call
//! before the server's `SETTINGS`, so that goes first, as soon as the connection is open.
//! Then the session acknowledges the client's `SETTINGS` and `PING`s, and answers each
//! stream that ends with `SERVING`:
//!
//! | Frame | Bytes |
//! | --- | --- |
//! | `SETTINGS`, empty | 9 |
//! | `SETTINGS` ACK | 9 |
//! | `HEADERS`: `:status 200`, `content-type: application/grpc` | 29 |
//! | `DATA`: `HealthCheckResponse { status: SERVING }` | 16 |
//! | `HEADERS` with `END_STREAM`: `grpc-status: 0` | 24 |
//!
//! The header blocks use only the static table and literals without indexing, so they need
//! no HPACK state, and nothing the client sends needs decoding.

/// What an HTTP/2 client sends first.
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const FRAME_HEADER_LEN: usize = 9;
/// The largest frame a client may send before the server allows larger ones.
const FRAME_MAX: usize = 16_384;

const DATA: u8 = 0x0;
const HEADERS: u8 = 0x1;
const SETTINGS: u8 = 0x4;
const PING: u8 = 0x6;
const GOAWAY: u8 = 0x7;

const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const END_HEADERS: u8 = 0x4;

/// `:status: 200` (static table entry 8), then `content-type: application/grpc` as a
/// literal without indexing, its name from static table entry 31.
const RESPONSE_HEADERS: &[u8] = b"\x88\x0f\x10\x10application/grpc";
/// A gRPC message, not compressed, 2 bytes long: field 1 (`status`) is 1, `SERVING`.
const SERVING: &[u8] = &[0, 0, 0, 0, 2, 0x08, 0x01];
/// `grpc-status: 0` as a literal without indexing, with a new name.
const TRAILERS: &[u8] = b"\x00\x0bgrpc-status\x010";

/// One h2c connection of a gRPC health probe, without IO.
#[derive(Debug, Default)]
pub struct GrpcSession {
    input: Vec<u8>,
    preface: bool,
}

impl GrpcSession {
    /// The server's connection preface, an empty `SETTINGS` frame, to send as soon as the
    /// connection is open.
    pub fn start(out: &mut Vec<u8>) {
        frame(out, SETTINGS, 0, 0, &[]);
    }

    /// Takes bytes from the client and appends the answers to `out`. Returns true once the
    /// client sent `GOAWAY`. An error means that the client does not speak h2c, or sent a
    /// frame too large for a probe.
    pub fn receive(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> Result<bool, String> {
        self.input.extend_from_slice(bytes);
        let mut at = 0;
        if !self.preface {
            let have = self.input.len().min(PREFACE.len());
            if self.input[..have] != PREFACE[..have] {
                return Err("not an HTTP/2 connection preface".into());
            }
            if have < PREFACE.len() {
                return Ok(false);
            }
            self.preface = true;
            at = PREFACE.len();
        }
        let mut goaway = false;
        while let Some(header) = self.input.get(at..at + FRAME_HEADER_LEN) {
            let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            if len > FRAME_MAX {
                return Err(format!("an HTTP/2 frame of {len} bytes"));
            }
            let start = at + FRAME_HEADER_LEN;
            let Some(payload) = self.input.get(start..start + len) else {
                break;
            };
            let (kind, flags) = (header[3], header[4]);
            let stream =
                u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
            match kind {
                SETTINGS if flags & ACK == 0 => frame(out, SETTINGS, ACK, 0, &[]),
                PING if flags & ACK == 0 => frame(out, PING, ACK, 0, payload),
                DATA if flags & END_STREAM != 0 && stream != 0 => answer(out, stream),
                GOAWAY => goaway = true,
                _ => {}
            }
            at = start + len;
        }
        self.input.drain(..at);
        Ok(goaway)
    }
}

/// The answer to a `Health/Check` call on `stream`: `SERVING`.
fn answer(out: &mut Vec<u8>, stream: u32) {
    frame(out, HEADERS, END_HEADERS, stream, RESPONSE_HEADERS);
    frame(out, DATA, 0, stream, SERVING);
    frame(out, HEADERS, END_HEADERS | END_STREAM, stream, TRAILERS);
}

fn frame(out: &mut Vec<u8>, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    let len = payload.len() as u32;
    out.extend_from_slice(&len.to_be_bytes()[1..]);
    out.extend_from_slice(&[kind, flags]);
    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(payload);
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    // The `Health/Check` call as kubelet makes it (`kube-probe/1.35`, no service),
    // captured from grpc-go 1.72.2.
    const CLIENT_PREFACE: &str = "505249202a20485454502f322e300d0a0d0a534d0d0a0d0a";
    const CLIENT_SETTINGS: &str = "000000040000000000";
    const CLIENT_SETTINGS_ACK: &str = "000000040100000000";
    const CLIENT_HEADERS: &str = concat!(
        "000068010400000001",
        "83864595626b2b22f394742675fb857c651d099d8bd3949d7f418b089d5c0b8170dc0bc0785f5f8b1d75",
        "d0620d263d4c4d65647a96eb6c655abb0f19580aecb6a4d65645a63b015dd12e2f40027465864d833505",
        "b11f40899acac8b24d494f6a7f867df79e6856ff",
    );
    const CLIENT_DATA: &str = concat!("000005000100000001", "0000000000");
    const CLIENT_WINDOW_UPDATE: &str = concat!("000004080000000000", "00000007");
    const CLIENT_PING: &str = concat!("000008060000000000", "02041010090e0707");
    const CLIENT_GOAWAY: &str = concat!(
        "000021070000000000",
        "0000000100000000636c69656e74207472616e73706f72742073687574646f776e",
    );

    /// The 87 bytes that grpc-go 1.72.2 accepts as `SERVING` on stream 1.
    pub const SERVER_REPLY: &str = concat!(
        "000000040000000000",
        "000000040100000000",
        "000014010400000001",
        "880f1010",
        "6170706c69636174696f6e2f67727063",
        "000007000000000001",
        "00000000020801",
        "00000f010500000001",
        "000b",
        "677270632d737461747573",
        "01",
        "30",
    );
    /// The acknowledgement of grpc-go's BDP ping.
    pub const SERVER_PING_ACK: &str = concat!("000008060100000000", "02041010090e0707");

    pub fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    /// What grpc-go sends up to the end of its call.
    pub fn client_call() -> Vec<u8> {
        [
            CLIENT_PREFACE,
            CLIENT_SETTINGS,
            CLIENT_SETTINGS_ACK,
            CLIENT_HEADERS,
            CLIENT_DATA,
        ]
        .iter()
        .flat_map(|h| hex(h))
        .collect()
    }

    /// What grpc-go sends after the answer: a window update, a BDP ping and `GOAWAY`.
    pub fn client_close() -> Vec<u8> {
        [CLIENT_WINDOW_UPDATE, CLIENT_PING, CLIENT_GOAWAY]
            .iter()
            .flat_map(|h| hex(h))
            .collect()
    }

    #[test]
    fn grpc_go_gets_the_87_bytes_it_accepted() {
        let mut out = Vec::new();
        GrpcSession::start(&mut out);
        let mut session = GrpcSession::default();
        assert_eq!(session.receive(&client_call(), &mut out), Ok(false));
        assert_eq!(out.len(), 87);
        assert_eq!(out, hex(SERVER_REPLY));

        let mut more = Vec::new();
        assert_eq!(session.receive(&client_close(), &mut more), Ok(true));
        assert_eq!(more, hex(SERVER_PING_ACK));
    }

    #[test]
    fn frames_may_arrive_a_byte_at_a_time() {
        let mut out = Vec::new();
        let mut session = GrpcSession::default();
        for byte in client_call() {
            assert_eq!(session.receive(&[byte], &mut out), Ok(false));
        }
        assert_eq!(out, hex(SERVER_REPLY)[9..]);
    }

    #[test]
    fn each_call_is_answered_on_its_stream() {
        let mut out = Vec::new();
        let mut call = hex(CLIENT_PREFACE);
        call.extend(hex(concat!("000005000100000003", "0000000000")));
        GrpcSession::default().receive(&call, &mut out).unwrap();
        assert_eq!(out.len(), 69);
        for at in [0, 29, 45] {
            assert_eq!(out[at + 5..at + 9], [0, 0, 0, 3], "frame at {at}");
        }
    }

    #[test]
    fn what_is_not_h2c_is_refused() {
        let mut out = Vec::new();
        let request = b"GET / HTTP/1.1\r\n\r\n";
        assert!(GrpcSession::default().receive(request, &mut out).is_err());
        let mut huge = hex(CLIENT_PREFACE);
        huge.extend(hex("004001000000000001"));
        assert!(GrpcSession::default().receive(&huge, &mut out).is_err());
        assert!(out.is_empty());
    }
}
