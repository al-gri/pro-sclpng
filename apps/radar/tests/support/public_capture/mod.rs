//! Synthetic fixtures and implementation-integrated source/allocator probes only.
//! Tracer is deliberately NOT a production allocator or whole-application bound.
//! Its single-thread diagnostic interval excludes retained baseline; abort denies
//! before System but cannot return through a recording-owner callback.
const CERT: &[u8] = &[
    48, 130, 3, 30, 48, 130, 2, 6, 160, 3, 2, 1, 2, 2, 20, 65, 138, 46, 208, 202, 133, 218, 68, 42,
    5, 109, 249, 142, 137, 159, 123, 75, 3, 123, 163, 48, 13, 6, 9, 42, 134, 72, 134, 247, 13, 1,
    1, 11, 5, 0, 48, 20, 49, 18, 48, 16, 6, 3, 85, 4, 3, 12, 9, 108, 111, 99, 97, 108, 104, 111,
    115, 116, 48, 32, 23, 13, 50, 54, 49, 48, 49, 48, 49, 51, 48, 56, 51, 53, 90, 24, 15, 50, 49,
    50, 54, 48, 57, 49, 54, 49, 51, 48, 56, 51, 53, 90, 48, 20, 49, 18, 48, 16, 6, 3, 85, 4, 3, 12,
    9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 48, 130, 1, 34, 48, 13, 6, 9, 42, 134, 72, 134,
    247, 13, 1, 1, 1, 5, 0, 3, 130, 1, 15, 0, 48, 130, 1, 10, 2, 130, 1, 1, 0, 215, 250, 180, 8,
    35, 100, 66, 202, 139, 110, 208, 38, 28, 30, 22, 190, 178, 214, 138, 99, 213, 144, 156, 220,
    101, 66, 114, 250, 132, 233, 94, 131, 118, 211, 32, 144, 87, 133, 236, 190, 122, 182, 138, 233,
    188, 69, 194, 220, 93, 155, 246, 105, 236, 126, 166, 209, 247, 47, 120, 176, 2, 72, 180, 23,
    18, 222, 150, 107, 212, 175, 85, 146, 250, 182, 159, 209, 94, 236, 162, 87, 138, 36, 234, 245,
    154, 219, 23, 152, 135, 185, 46, 34, 140, 127, 251, 212, 16, 158, 222, 1, 82, 225, 224, 119,
    139, 106, 91, 236, 149, 108, 0, 129, 245, 159, 210, 18, 72, 49, 85, 223, 62, 67, 94, 46, 60,
    132, 141, 182, 216, 120, 89, 32, 6, 195, 211, 110, 204, 69, 34, 99, 104, 202, 205, 59, 153, 71,
    90, 179, 178, 255, 195, 62, 42, 241, 141, 38, 175, 129, 235, 235, 148, 180, 80, 75, 245, 91,
    250, 143, 26, 66, 136, 114, 3, 32, 132, 235, 37, 191, 164, 97, 156, 75, 70, 63, 99, 12, 11,
    173, 235, 54, 79, 41, 1, 72, 45, 204, 154, 140, 16, 81, 120, 14, 164, 20, 119, 109, 77, 89,
    246, 240, 162, 14, 198, 127, 149, 56, 52, 222, 28, 220, 126, 61, 59, 136, 65, 240, 208, 195,
    120, 37, 90, 154, 31, 139, 150, 51, 198, 235, 172, 160, 13, 59, 41, 121, 201, 29, 26, 3, 221,
    26, 44, 254, 1, 8, 49, 145, 2, 3, 1, 0, 1, 163, 102, 48, 100, 48, 29, 6, 3, 85, 29, 14, 4, 22,
    4, 20, 152, 237, 225, 178, 21, 132, 128, 168, 151, 53, 151, 80, 194, 123, 124, 213, 4, 65, 166,
    24, 48, 31, 6, 3, 85, 29, 35, 4, 24, 48, 22, 128, 20, 152, 237, 225, 178, 21, 132, 128, 168,
    151, 53, 151, 80, 194, 123, 124, 213, 4, 65, 166, 24, 48, 20, 6, 3, 85, 29, 17, 4, 13, 48, 11,
    130, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 48, 12, 6, 3, 85, 29, 19, 1, 1, 255, 4, 2,
    48, 0, 48, 13, 6, 9, 42, 134, 72, 134, 247, 13, 1, 1, 11, 5, 0, 3, 130, 1, 1, 0, 46, 91, 183,
    76, 24, 123, 66, 179, 24, 247, 190, 202, 250, 29, 211, 211, 105, 240, 172, 232, 247, 136, 43,
    90, 56, 53, 126, 221, 238, 237, 116, 15, 56, 64, 121, 174, 96, 221, 96, 81, 68, 137, 24, 197,
    252, 143, 236, 102, 112, 75, 153, 107, 112, 53, 241, 36, 227, 83, 211, 120, 139, 109, 0, 45,
    144, 220, 140, 55, 45, 71, 65, 97, 38, 212, 69, 130, 153, 251, 242, 99, 211, 0, 237, 230, 147,
    12, 2, 119, 176, 171, 197, 113, 226, 38, 143, 245, 161, 102, 161, 97, 80, 10, 245, 18, 99, 150,
    83, 161, 23, 45, 54, 12, 6, 234, 160, 234, 206, 106, 87, 79, 232, 221, 45, 149, 218, 200, 108,
    140, 32, 179, 218, 89, 211, 115, 75, 66, 98, 46, 89, 193, 204, 130, 139, 112, 197, 91, 96, 133,
    181, 130, 214, 244, 99, 2, 236, 162, 64, 198, 65, 105, 67, 2, 81, 42, 124, 244, 182, 127, 4,
    62, 88, 116, 191, 12, 219, 223, 42, 115, 206, 160, 181, 112, 27, 235, 42, 132, 37, 248, 230,
    247, 83, 78, 161, 63, 232, 89, 201, 62, 21, 49, 72, 68, 192, 28, 251, 43, 42, 147, 169, 43, 21,
    53, 167, 120, 14, 65, 109, 130, 27, 116, 191, 117, 142, 84, 85, 89, 255, 100, 142, 218, 63, 85,
    61, 218, 26, 155, 212, 48, 3, 1, 244, 40, 94, 91, 67, 0, 199, 8, 105, 136, 85, 12, 230, 128,
    53, 211,
];
const KEY: &[u8] = &[
    48, 130, 4, 188, 2, 1, 0, 48, 13, 6, 9, 42, 134, 72, 134, 247, 13, 1, 1, 1, 5, 0, 4, 130, 4,
    166, 48, 130, 4, 162, 2, 1, 0, 2, 130, 1, 1, 0, 215, 250, 180, 8, 35, 100, 66, 202, 139, 110,
    208, 38, 28, 30, 22, 190, 178, 214, 138, 99, 213, 144, 156, 220, 101, 66, 114, 250, 132, 233,
    94, 131, 118, 211, 32, 144, 87, 133, 236, 190, 122, 182, 138, 233, 188, 69, 194, 220, 93, 155,
    246, 105, 236, 126, 166, 209, 247, 47, 120, 176, 2, 72, 180, 23, 18, 222, 150, 107, 212, 175,
    85, 146, 250, 182, 159, 209, 94, 236, 162, 87, 138, 36, 234, 245, 154, 219, 23, 152, 135, 185,
    46, 34, 140, 127, 251, 212, 16, 158, 222, 1, 82, 225, 224, 119, 139, 106, 91, 236, 149, 108, 0,
    129, 245, 159, 210, 18, 72, 49, 85, 223, 62, 67, 94, 46, 60, 132, 141, 182, 216, 120, 89, 32,
    6, 195, 211, 110, 204, 69, 34, 99, 104, 202, 205, 59, 153, 71, 90, 179, 178, 255, 195, 62, 42,
    241, 141, 38, 175, 129, 235, 235, 148, 180, 80, 75, 245, 91, 250, 143, 26, 66, 136, 114, 3, 32,
    132, 235, 37, 191, 164, 97, 156, 75, 70, 63, 99, 12, 11, 173, 235, 54, 79, 41, 1, 72, 45, 204,
    154, 140, 16, 81, 120, 14, 164, 20, 119, 109, 77, 89, 246, 240, 162, 14, 198, 127, 149, 56, 52,
    222, 28, 220, 126, 61, 59, 136, 65, 240, 208, 195, 120, 37, 90, 154, 31, 139, 150, 51, 198,
    235, 172, 160, 13, 59, 41, 121, 201, 29, 26, 3, 221, 26, 44, 254, 1, 8, 49, 145, 2, 3, 1, 0, 1,
    2, 130, 1, 0, 20, 83, 145, 14, 188, 132, 246, 183, 192, 254, 99, 122, 216, 181, 216, 186, 103,
    18, 163, 168, 224, 108, 57, 37, 17, 103, 26, 242, 218, 160, 43, 127, 216, 38, 167, 215, 137,
    211, 45, 252, 33, 200, 173, 3, 113, 47, 136, 22, 172, 253, 45, 8, 92, 68, 113, 175, 12, 82, 84,
    139, 141, 21, 122, 84, 123, 23, 69, 133, 29, 185, 200, 38, 26, 112, 38, 83, 162, 57, 71, 80,
    145, 140, 230, 171, 137, 128, 209, 228, 78, 46, 182, 137, 180, 27, 37, 240, 88, 5, 153, 155,
    122, 137, 77, 82, 182, 229, 98, 68, 30, 110, 113, 16, 74, 21, 121, 227, 244, 85, 39, 81, 168,
    182, 150, 166, 39, 150, 42, 143, 132, 230, 144, 137, 50, 121, 96, 88, 171, 126, 108, 176, 233,
    45, 36, 42, 229, 104, 215, 71, 98, 95, 227, 50, 75, 177, 194, 140, 169, 255, 99, 239, 50, 215,
    243, 89, 169, 151, 134, 3, 252, 112, 65, 253, 156, 155, 37, 189, 199, 114, 83, 219, 0, 222, 10,
    2, 58, 205, 75, 2, 7, 131, 62, 98, 8, 13, 210, 31, 72, 235, 38, 144, 212, 223, 107, 192, 99,
    188, 118, 177, 178, 123, 31, 9, 165, 191, 34, 245, 37, 206, 47, 14, 59, 61, 205, 136, 86, 118,
    123, 14, 200, 187, 56, 233, 168, 193, 193, 130, 113, 127, 146, 74, 40, 250, 107, 126, 89, 70,
    96, 52, 177, 52, 124, 72, 249, 249, 2, 185, 2, 129, 129, 0, 235, 247, 220, 56, 167, 239, 8, 39,
    125, 206, 242, 12, 196, 159, 247, 12, 63, 207, 134, 246, 61, 56, 182, 24, 63, 96, 126, 60, 131,
    136, 246, 139, 203, 208, 255, 215, 177, 80, 113, 237, 122, 215, 224, 69, 72, 188, 71, 75, 81,
    230, 125, 247, 209, 48, 183, 191, 58, 204, 161, 11, 251, 115, 224, 64, 126, 108, 151, 18, 186,
    108, 199, 211, 178, 145, 151, 160, 68, 90, 154, 144, 152, 206, 19, 50, 114, 85, 75, 79, 235,
    33, 239, 20, 64, 70, 172, 127, 20, 221, 159, 81, 68, 37, 118, 88, 25, 169, 159, 16, 160, 88,
    168, 23, 207, 36, 113, 241, 39, 66, 130, 54, 198, 189, 240, 226, 239, 255, 206, 137, 2, 129,
    129, 0, 234, 80, 112, 12, 8, 234, 207, 74, 210, 0, 253, 158, 217, 191, 165, 38, 101, 141, 71,
    15, 1, 175, 163, 174, 55, 207, 73, 19, 202, 17, 197, 106, 139, 56, 103, 19, 57, 51, 1, 141, 58,
    220, 22, 193, 114, 191, 105, 227, 62, 198, 249, 69, 213, 212, 195, 234, 209, 126, 206, 238,
    184, 125, 43, 151, 53, 189, 225, 17, 117, 228, 80, 167, 44, 244, 194, 180, 234, 210, 205, 194,
    39, 113, 145, 101, 155, 60, 142, 227, 77, 57, 44, 65, 85, 0, 155, 115, 83, 134, 224, 207, 152,
    2, 255, 161, 99, 222, 180, 222, 242, 86, 210, 172, 180, 74, 241, 55, 13, 122, 119, 161, 178,
    121, 180, 58, 164, 52, 200, 201, 2, 129, 128, 120, 226, 99, 153, 74, 190, 243, 232, 119, 85,
    27, 63, 91, 67, 175, 230, 64, 146, 106, 75, 159, 149, 124, 3, 244, 3, 212, 231, 223, 98, 189,
    27, 100, 240, 207, 0, 138, 191, 241, 125, 125, 159, 54, 47, 136, 81, 156, 28, 131, 250, 150,
    177, 236, 35, 15, 31, 18, 90, 94, 110, 171, 4, 243, 239, 86, 84, 255, 24, 3, 21, 83, 81, 170,
    123, 87, 184, 45, 12, 85, 126, 154, 41, 136, 64, 33, 190, 124, 116, 150, 186, 173, 166, 44, 63,
    136, 131, 26, 7, 103, 100, 212, 138, 116, 148, 49, 161, 105, 241, 180, 147, 118, 153, 171, 238,
    185, 200, 151, 26, 69, 103, 22, 109, 156, 8, 70, 119, 64, 49, 2, 129, 128, 96, 168, 124, 0, 74,
    241, 106, 63, 200, 47, 198, 111, 240, 13, 129, 184, 60, 46, 50, 128, 251, 70, 20, 52, 123, 43,
    84, 79, 8, 141, 154, 45, 160, 110, 204, 254, 126, 27, 15, 105, 206, 61, 26, 90, 4, 214, 247,
    124, 89, 218, 68, 220, 77, 32, 111, 13, 128, 12, 90, 154, 217, 154, 49, 16, 56, 136, 50, 191,
    60, 45, 202, 35, 156, 132, 255, 137, 24, 81, 139, 181, 171, 5, 203, 95, 233, 208, 234, 116,
    211, 215, 96, 237, 54, 126, 128, 161, 235, 115, 249, 107, 73, 158, 251, 10, 253, 162, 210, 100,
    33, 254, 52, 252, 47, 135, 182, 199, 234, 20, 122, 35, 70, 247, 179, 164, 121, 54, 153, 25, 2,
    129, 128, 86, 33, 71, 26, 99, 88, 227, 108, 132, 175, 55, 42, 117, 252, 229, 69, 146, 207, 158,
    149, 205, 194, 141, 251, 230, 102, 107, 178, 209, 38, 203, 115, 245, 61, 246, 250, 180, 251,
    105, 2, 75, 236, 30, 196, 141, 92, 227, 152, 68, 185, 23, 72, 151, 249, 71, 253, 166, 50, 124,
    30, 2, 102, 115, 199, 22, 34, 37, 161, 63, 239, 20, 151, 20, 52, 196, 101, 46, 171, 72, 33,
    144, 154, 228, 254, 176, 180, 93, 226, 6, 42, 249, 138, 234, 255, 151, 186, 222, 165, 70, 219,
    197, 94, 174, 197, 245, 6, 106, 48, 167, 130, 52, 216, 130, 0, 169, 202, 181, 146, 7, 4, 194,
    180, 69, 12, 50, 11, 44, 75,
];
mod transport_probes {
    // Synthetic scratch gate probe. No production recording owner or network capture.
    use std::cell::Cell;
    use std::collections::HashSet;
    use std::io::{self, Cursor, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection};
    use tungstenite::client::IntoClientRequest;
    use tungstenite::handshake::derive_accept_key;
    use tungstenite::protocol::frame::coding::{Control, Data, OpCode};
    use tungstenite::protocol::frame::{Frame, FrameSocket};

    static WATCH: AtomicBool = AtomicBool::new(false);
    static ALLOCS: AtomicUsize = AtomicUsize::new(0);
    static MAX_REQUEST: AtomicUsize = AtomicUsize::new(0);
    pub(super) fn watch_request(size: usize) {
        if WATCH.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            MAX_REQUEST.fetch_max(size, Ordering::Relaxed);
        }
    }
    fn text_frame(data: &[u8]) -> Frame {
        Frame::message(data.to_vec(), OpCode::Data(Data::Text), true)
    }
    fn masked_text(data: &[u8]) -> (Frame, [u8; 4]) {
        let frame =
            crate::transport::masked_text(data, &rustls::crypto::ring::default_provider()).unwrap();
        let mask = frame.header().mask.unwrap();
        (frame, mask)
    }

    fn masks() {
        let mut seen = HashSet::new();
        let payload = b"synthetic-mask-probe";
        for _ in 0..64 {
            let (frame, mask) = masked_text(payload);
            assert!(
                seen.insert(mask),
                "unexpected random collision in 64 sample masks"
            );
            let mut sock = FrameSocket::new(Vec::new());
            sock.write(frame).unwrap();
            sock.flush().unwrap();
            let (wire, _) = sock.into_inner();
            assert_eq!(wire[0], 0x81);
            assert_eq!(wire[1], 0x80 | payload.len() as u8);
            assert_eq!(&wire[2..6], &mask);
            for (i, &byte) in payload.iter().enumerate() {
                assert_eq!(wire[6 + i] ^ mask[i % 4], byte);
            }
        }
        println!(
            "PASS masks: 64 fresh secure_random calls, public header.mask, 64 distinct sample masks, serialized XOR exact; freshness proof is selected provider, uniqueness is sample observation"
        );
    }

    #[derive(Default)]
    struct PartialSink {
        wire: Vec<u8>,
        calls: usize,
        allow: usize,
        flush_blocks: usize,
        flush_calls: usize,
    }
    impl Write for PartialSink {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            if self.allow == 0 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let n = b.len().min(self.allow);
            self.wire.extend_from_slice(&b[..n]);
            self.allow -= n;
            Ok(n)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flush_calls += 1;
            if self.flush_blocks > 0 {
                self.flush_blocks -= 1;
                return Err(io::ErrorKind::WouldBlock.into());
            }
            Ok(())
        }
    }
    fn is_would_block(result: &tungstenite::Result<()>) -> bool {
        matches!(result, Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock)
    }
    fn partial_write() {
        let (frame, _) = masked_text(b"synthetic-once");
        let mut expected = Vec::new();
        frame.clone().format(&mut expected).unwrap();
        let mut sock = FrameSocket::new(PartialSink {
            allow: 3,
            ..Default::default()
        });
        let first = sock.write(frame); // The ONLY enqueue of this frame.
        assert!(is_would_block(&first));
        assert_eq!(sock.get_ref().wire.len(), 3);
        assert!(is_would_block(&sock.flush()));
        assert_eq!(sock.get_ref().wire.len(), 3);
        sock.get_mut().allow = 1000;
        sock.get_mut().flush_blocks = 1;
        assert!(is_would_block(&sock.flush()));
        assert_eq!(sock.get_ref().wire, expected);
        let calls_before = sock.get_ref().calls;
        sock.flush().unwrap();
        assert_eq!(sock.get_ref().calls, calls_before);
        assert_eq!(sock.get_ref().wire, expected);
        println!(
            "PASS partial_write: enqueue=1; first write=3 bytes then WouldBlock; blocked flush=0 more; next flush completes same frame then underlying flush WouldBlock; final flush adds 0 bytes, wire={} bytes, physical flush calls={}",
            expected.len(),
            sock.get_ref().flush_calls
        );
    }

    struct Duplex {
        incoming: Cursor<Vec<u8>>,
        writes: usize,
        read_calls: usize,
    }
    impl Read for Duplex {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.read_calls += 1;
            self.incoming.read(b)
        }
    }
    impl Write for Duplex {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            self.writes += 1;
            Ok(())
        }
    }
    fn validate_text(frame: &Frame) -> Result<(), &'static str> {
        crate::transport::validate_text(frame).map(|_| ())
    }

    fn raw_passive_and_refusals() {
        for (wire, opcode) in [
            (vec![0x89, 1, b'P'], OpCode::Control(Control::Ping)),
            (vec![0x88, 0], OpCode::Control(Control::Close)),
            (vec![0x8a, 1, b'P'], OpCode::Control(Control::Pong)),
        ] {
            let stream = Duplex {
                incoming: Cursor::new(wire),
                writes: 0,
                read_calls: 0,
            };
            let mut sock = FrameSocket::new(stream);
            let frame = sock.read(Some(65536)).unwrap().unwrap();
            assert_eq!(frame.header().opcode, opcode);
            assert_eq!(
                validate_text(&frame),
                Err("unsupported opcode; native controls fail-stop")
            );
            assert_eq!(sock.get_ref().writes, 0);
        }
        let fixtures = [
            (vec![0x01, 1, b'a'], "unsupported frame header"),
            (vec![0xc1, 1, b'a'], "unsupported frame header"),
            (
                vec![0x82, 1, b'a'],
                "unsupported opcode; native controls fail-stop",
            ),
            (vec![0x81, 1, 0xff], "invalid UTF8"),
            (
                vec![0x81, 0x81, 1, 2, 3, 4, b'a' ^ 1],
                "unsupported frame header",
            ),
        ];
        for (wire, reason) in fixtures {
            let mut sock = FrameSocket::new(Cursor::new(wire));
            assert_eq!(
                validate_text(&sock.read(Some(65536)).unwrap().unwrap()),
                Err(reason)
            );
        }
        println!(
            "PASS passive_raw: Ping/Pong/Close writes=0; explicit FIN/RSV/masked-server/binary/UTF8/native-opcode refusal checks"
        );
    }
    fn oversize_before_payload_reserve() {
        for length in [65537_u64, u64::MAX] {
            let mut header = vec![0x81, 0x7f];
            header.extend_from_slice(&length.to_be_bytes());
            let mut sock = FrameSocket::new(Duplex {
                incoming: Cursor::new(header),
                writes: 0,
                read_calls: 0,
            });
            ALLOCS.store(0, Ordering::Relaxed);
            MAX_REQUEST.store(0, Ordering::Relaxed);
            WATCH.store(true, Ordering::Relaxed);
            let result = sock.read(Some(65536));
            WATCH.store(false, Ordering::Relaxed);
            assert!(matches!(result, Err(tungstenite::Error::Capacity(_))));
            assert_eq!(sock.get_ref().read_calls, 1);
            assert_eq!(ALLOCS.load(Ordering::Relaxed), 0);
            let (_, ingress) = sock.into_inner();
            println!(
                "PASS oversized_advertisement: length={length}, cap=65536, read_calls=1, allocation_requests_during_read=0, max_request=0, initial_ingress=131072, post_header_visible_capacity={}",
                ingress.capacity()
            );
        }
    }

    struct SlowRead {
        bytes: Cursor<Vec<u8>>,
        now: Rc<Cell<u64>>,
        next_deadline: Rc<Cell<u64>>,
        blocked: bool,
        observed: usize,
    }
    impl Read for SlowRead {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if self.now.get() >= self.next_deadline.get() {
                return Err(io::ErrorKind::TimedOut.into());
            }
            if self.blocked {
                self.blocked = false;
                return Err(io::ErrorKind::WouldBlock.into());
            }
            self.blocked = true;
            let n = self.bytes.read(&mut out[..1])?;
            if n > 0 {
                self.observed += n;
                self.now.set(self.now.get() + 50);
            }
            Ok(n)
        }
    }
    struct SlowWrite {
        now: Rc<Cell<u64>>,
        deadline: u64,
        blocked: bool,
        wire: Vec<u8>,
    }
    impl Write for SlowWrite {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            if self.now.get() >= self.deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            if self.blocked {
                self.blocked = false;
                return Err(io::ErrorKind::WouldBlock.into());
            }
            self.blocked = true;
            self.wire.push(b[0]);
            self.now.set(self.now.get() + 200);
            Ok(1)
        }
        fn flush(&mut self) -> io::Result<()> {
            if self.now.get() >= self.deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            Ok(())
        }
    }
    fn absolute_deadlines() {
        let now = Rc::new(Cell::new(0));
        let deadline = Rc::new(Cell::new(2000)); // initial observed progress: 0; one immutable +2s deadline
        let mut bytes = vec![0x81, 100];
        bytes.extend_from_slice(&[b's'; 100]);
        let mut sock = FrameSocket::new(SlowRead {
            bytes: Cursor::new(bytes),
            now: now.clone(),
            next_deadline: deadline,
            blocked: false,
            observed: 0,
        });
        let mut quanta = 0;
        loop {
            let before = now.get();
            match sock.read(Some(65536)) {
                Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::TimedOut => break,
                other => panic!("unexpected slow read result: {other:?}"),
            }
            assert!(now.get() - before <= 50);
            quanta += 1;
        }
        assert_eq!(now.get(), 2000);
        assert_eq!(sock.get_ref().observed, 40);
        println!(
            "PASS slow_drip_read: synthetic clock, initial progress=0ms, immutable deadline=2000ms, stopped=2000ms, bytes=40/102, quanta={quanta}, each<=50ms, no deadline renewal"
        );
        let now = Rc::new(Cell::new(0));
        let mut sock = FrameSocket::new(SlowWrite {
            now: now.clone(),
            deadline: 1000,
            blocked: false,
            wire: Vec::new(),
        });
        let (frame, _) = masked_text(b"pending-at-original-deadline");
        assert!(is_would_block(&sock.write(frame))); // only enqueue
        loop {
            match sock.flush() {
                Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::TimedOut => break,
                other => panic!("unexpected slow write result: {other:?}"),
            }
        }
        assert_eq!(now.get(), 1000);
        assert_eq!(sock.get_ref().wire.len(), 5);
        println!(
            "PASS slow_drip_write: synthetic clock, original deadline=1000ms, stopped=1000ms, enqueue=1, physical partial bytes=5, result=Unknown/pending, no resend or renewed deadline"
        );
        // Original connect deadline stays min(stage original bound, total original bound).
        let origin = 100_u64;
        let tcp_end = origin + 2000;
        let tls_started = 1900_u64;
        let tls_end = (tls_started + 3000).min(origin + 7000);
        let upgrade_started = 4800_u64;
        let upgrade_end = (upgrade_started + 2000).min(origin + 7000);
        assert_eq!((tcp_end, tls_end, upgrade_end), (2100, 4900, 6800));
        println!(
            "PASS connect_deadline_arithmetic: original total origin+7000ms, stage TCP2s/TLS3s/upgrade2s absolute, no resets on partial progress; concrete real elapsed times separately recorded"
        );
    }

    struct BoundedSlowTcp {
        stream: TcpStream,
        first_progress: Option<Instant>,
        frame_deadline: Option<Instant>,
        quantum_deadline: Instant,
        observed: usize,
    }
    impl Read for BoundedSlowTcp {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let now = Instant::now();
            if self.frame_deadline.is_some_and(|d| now >= d) {
                return Err(io::ErrorKind::TimedOut.into());
            }
            if now >= self.quantum_deadline {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let end = self
                .frame_deadline
                .map_or(self.quantum_deadline, |d| d.min(self.quantum_deadline));
            self.stream.set_read_timeout(Some(end - now))?;
            match self.stream.read(out) {
                Ok(n) => {
                    if n > 0 {
                        self.observed += n;
                        if self.first_progress.is_none() {
                            let origin = Instant::now();
                            self.first_progress = Some(origin);
                            self.frame_deadline = Some(origin + Duration::from_secs(2));
                        }
                    }
                    Ok(n)
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    if self.frame_deadline.is_some_and(|d| Instant::now() >= d) {
                        Err(io::ErrorKind::TimedOut.into())
                    } else {
                        Err(io::ErrorKind::WouldBlock.into())
                    }
                }
                Err(e) => Err(e),
            }
        }
    }
    fn real_slow_drip_read_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let receiver =
            TcpStream::connect_timeout(&listener.local_addr().unwrap(), Duration::from_secs(2))
                .unwrap();
        let (mut sender, _) = listener.accept().unwrap();
        sender.set_nodelay(true).unwrap();
        let worker = std::thread::spawn(move || {
            let mut wire = vec![0x81, 100];
            wire.extend_from_slice(&[b's'; 100]);
            for byte in wire {
                if sender.write_all(&[byte]).is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(49));
            }
        });
        let mut sock = FrameSocket::new(BoundedSlowTcp {
            stream: receiver,
            first_progress: None,
            frame_deadline: None,
            quantum_deadline: Instant::now() + Duration::from_millis(10),
            observed: 0,
        });
        let mut max_quantum_us = 0;
        loop {
            sock.get_mut().quantum_deadline = Instant::now() + Duration::from_millis(10);
            let start = Instant::now();
            let result = sock.read(Some(65536));
            max_quantum_us = max_quantum_us.max(start.elapsed().as_micros());
            match result {
                Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::TimedOut => break,
                other => panic!("unexpected real slow drip: {other:?}"),
            }
        }
        let elapsed_ms = sock.get_ref().first_progress.unwrap().elapsed().as_millis();
        println!(
            "TRACE actual_TCP_slow_drip: original_frame_deadline_ms=2000, planned_service_quantum_ms=10, elapsed_ms={elapsed_ms}, max_service_quantum_us={max_quantum_us}, bytes={}",
            sock.get_ref().observed
        );
        assert!((2000..=2100).contains(&elapsed_ms));
        assert!(max_quantum_us <= 50000);
        assert!(sock.get_ref().observed < 102);
        let observed = sock.get_ref().observed;
        drop(sock);
        worker.join().unwrap();
        println!(
            "PASS actual_TCP_slow_drip: numeric127.0.0.1, peer sends one byte/49ms, original first-progress+2000ms deadline, elapsed_ms={elapsed_ms}, bytes={observed}/102, max_service_quantum_us={max_quantum_us}<=50000; progress never renews frame deadline"
        );
    }

    fn configs(
        version: &'static rustls::SupportedProtocolVersion,
    ) -> (Arc<ClientConfig>, Arc<ServerConfig>) {
        let cert = CertificateDer::from(super::CERT.to_vec());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(super::KEY.to_vec()));
        let mut roots = RootCertStore::empty();
        roots.add(cert.clone()).unwrap();
        let mut client =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[version])
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        client.resumption = rustls::client::Resumption::disabled();
        client.enable_early_data = false;
        let mut server =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[version])
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .unwrap();
        server.send_tls13_tickets = 0;
        (Arc::new(client), Arc::new(server))
    }
    fn drain_client(client: &mut ClientConnection, wire: &mut Vec<u8>, deadline: Instant) -> usize {
        let before = wire.len();
        while client.wants_write() {
            assert!(Instant::now() < deadline, "original TLS drain deadline");
            assert!(client.write_tls(wire).unwrap() > 0);
        }
        wire.flush().unwrap();
        wire.len() - before
    }
    fn drain_server(server: &mut ServerConnection, wire: &mut Vec<u8>, deadline: Instant) -> usize {
        let before = wire.len();
        while server.wants_write() {
            assert!(Instant::now() < deadline, "original TLS drain deadline");
            assert!(server.write_tls(wire).unwrap() > 0);
        }
        wire.flush().unwrap();
        wire.len() - before
    }
    fn deliver_to_server(server: &mut ServerConnection, wire: &[u8]) {
        let mut cursor = Cursor::new(wire);
        while cursor.position() < wire.len() as u64 {
            server.read_tls(&mut cursor).unwrap();
            server.process_new_packets().unwrap();
        }
    }
    fn deliver_to_client(client: &mut ClientConnection, wire: &[u8]) {
        let mut cursor = Cursor::new(wire);
        while cursor.position() < wire.len() as u64 {
            client.read_tls(&mut cursor).unwrap();
            client.process_new_packets().unwrap();
        }
    }
    fn tls_pair(
        version: &'static rustls::SupportedProtocolVersion,
    ) -> (ClientConnection, ServerConnection) {
        let (client_config, server_config) = configs(version);
        let mut client =
            ClientConnection::new(client_config, ServerName::try_from("localhost").unwrap())
                .unwrap();
        let mut server = ServerConnection::new(server_config).unwrap();
        client.set_buffer_limit(Some(65536));
        server.set_buffer_limit(Some(65536));
        let start = Instant::now();
        let deadline = start + Duration::from_secs(3);
        let mut post_handshake_client_drain = 0;
        for _ in 0..10 {
            let mut c_wire = Vec::new();
            if !client.is_handshaking() && client.wants_write() {
                post_handshake_client_drain += 1;
            }
            drain_client(&mut client, &mut c_wire, deadline);
            if !c_wire.is_empty() {
                deliver_to_server(&mut server, &c_wire);
            }
            let mut s_wire = Vec::new();
            drain_server(&mut server, &mut s_wire, deadline);
            if !s_wire.is_empty() {
                deliver_to_client(&mut client, &s_wire);
            }
            if !client.is_handshaking()
                && !server.is_handshaking()
                && !client.wants_write()
                && !server.wants_write()
            {
                break;
            }
        }
        assert!(!client.is_handshaking() && !server.is_handshaking());
        assert!(!client.wants_write() && !server.wants_write());
        if version == &rustls::version::TLS13 {
            assert!(post_handshake_client_drain > 0);
        }
        println!(
            "PASS TLS_handshake {:?}: elapsed_us={}, explicit post-is_handshaking-false client drain occurrences={}, residual wants_write=false, resumption disabled, early_data=false, selected ring, original TLS deadline=3000ms",
            client.protocol_version(),
            start.elapsed().as_micros(),
            post_handshake_client_drain
        );
        (client, server)
    }
    fn record_count(wire: &[u8]) -> usize {
        let mut offset = 0;
        let mut count = 0;
        while offset < wire.len() {
            assert!(offset + 5 <= wire.len());
            let n = u16::from_be_bytes([wire[offset + 3], wire[offset + 4]]) as usize;
            offset += 5 + n;
            count += 1;
        }
        assert_eq!(offset, wire.len());
        count
    }

    #[derive(Debug)]
    struct HeaderOnly {
        complete_header: Cursor<Vec<u8>>,
        tail: Vec<u8>,
        request: Vec<u8>,
        read_calls: usize,
    }
    impl Read for HeaderOnly {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.read_calls += 1;
            self.complete_header.read(b)
        }
    }
    impl Write for HeaderOnly {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            if self.request.len() + b.len() > 8192 {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            self.request.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn stage_response(plain: &[u8]) -> Result<HeaderOnly, &'static str> {
        // Prepaid fixed caps; refuse BEFORE growing either stage beyond its cap.
        let mut header = Vec::with_capacity(8192);
        let mut header_end = None;
        for (offset, &byte) in plain.iter().enumerate() {
            if header.len() == 8192 {
                return Err("HTTP header >8192");
            }
            header.push(byte);
            if header.ends_with(b"\r\n\r\n") {
                header_end = Some(offset + 1);
                break;
            }
        }
        let header_end = header_end.ok_or("incomplete")?;
        let tail_len = plain.len() - header_end;
        if tail_len > 65536 {
            return Err("prefetch >65536");
        }
        let mut tail = Vec::with_capacity(65536);
        tail.extend_from_slice(&plain[header_end..]);
        Ok(HeaderOnly {
            complete_header: Cursor::new(header),
            tail,
            request: Vec::new(),
            read_calls: 0,
        })
    }
    fn fixed_request() -> tungstenite::handshake::client::Request {
        let mut request = "wss://localhost/synthetic".into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Key",
            "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap(),
        );
        request
    }
    fn response(extra: &str) -> Vec<u8> {
        let accept = derive_accept_key(b"dGhlIHNhbXBsZSBub25jZQ==");
        format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n{extra}\r\n").into_bytes()
    }
    struct CountedTcp {
        stream: TcpStream,
        reads: usize,
        read_bytes: usize,
        writes: usize,
        remaining: usize,
    }
    impl Read for CountedTcp {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if self.remaining == 0 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            self.reads += 1;
            let cap = out.len().min(self.remaining).min(16384);
            let n = self.stream.read(&mut out[..cap])?;
            self.read_bytes += n;
            self.remaining -= n;
            Ok(n)
        }
    }
    impl Write for CountedTcp {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.stream.write(b)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.writes += 1;
            self.stream.flush()
        }
    }
    fn deliver_tls_on_tcp_loopback(client: &mut ClientConnection, wire: &[u8]) -> (usize, usize) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut sender =
            TcpStream::connect_timeout(&listener.local_addr().unwrap(), Duration::from_secs(2))
                .unwrap();
        let (receiver, _) = listener.accept().unwrap();
        sender
            .set_write_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        sender.write_all(wire).unwrap();
        sender.flush().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut counted = CountedTcp {
            stream: receiver,
            reads: 0,
            read_bytes: 0,
            writes: 0,
            remaining: wire.len(),
        };
        while counted.remaining > 0 {
            assert!(Instant::now() < deadline);
            assert!(client.read_tls(&mut counted).unwrap() > 0);
            client.process_new_packets().unwrap();
        }
        assert_eq!(counted.writes, 0);
        (counted.reads, counted.read_bytes)
    }
    fn http_and_tls_coalescing() {
        let (mut client, mut server) = tls_pair(&rustls::version::TLS13);
        let mut plaintext = response("");
        let header_len = plaintext.len();
        text_frame(b"synthetic-first-frame")
            .format(&mut plaintext)
            .unwrap();
        server.writer().write_all(&plaintext).unwrap();
        let mut wire = Vec::new();
        drain_server(
            &mut server,
            &mut wire,
            Instant::now() + Duration::from_secs(1),
        );
        assert_eq!(record_count(&wire), 1);
        assert!(!client.wants_write());
        let (tcp_reads, tcp_bytes) = deliver_tls_on_tcp_loopback(&mut client, &wire); // passive TLS; physical numeric loopback TCP
        assert!(!client.wants_write());
        let mut readbuf = [0; 4096];
        let n = client.reader().read(&mut readbuf).unwrap();
        assert_eq!(&readbuf[..n], &plaintext);
        let staged = stage_response(&readbuf[..n]).unwrap();
        let (ws, result) = tungstenite::client(fixed_request(), staged).unwrap();
        assert_eq!(result.status().as_u16(), 101);
        assert!(!result.headers().contains_key("Sec-WebSocket-Extensions"));
        let staged = ws.into_inner(); // library sees header ONLY; tail belongs to adapter.
        assert_eq!(staged.read_calls, 1);
        assert!(staged.request.len() <= 8192);
        let tail_len = staged.tail.len();
        let mut raw = FrameSocket::from_partially_read(Cursor::new(Vec::<u8>::new()), staged.tail);
        let frame = raw.read(Some(65536)).unwrap().unwrap();
        validate_text(&frame).unwrap();
        assert_eq!(frame.payload(), b"synthetic-first-frame");
        println!(
            "PASS HTTP101_first_frame: actual numeric TCP127.0.0.1 TLS1.3 loopback, one application TLS record={} ciphertext bytes, TCP read calls={tcp_reads}/bytes={tcp_bytes}, plaintext={} bytes, header={} bytes, preserved_tail={} bytes, library HTTP validation header Read calls=1, first Text exact, passive raw TLS writes=0/residual wants_write=false; TLS handshake flights are cryptographic memory loopback",
            wire.len(),
            n,
            header_len,
            tail_len
        );
        let (ws, result) = tungstenite::client(
            fixed_request(),
            stage_response(&response(
                "Sec-WebSocket-Extensions: permessage-deflate\r\n",
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(result.status().as_u16(), 101);
        assert!(result.headers().contains_key("Sec-WebSocket-Extensions"));
        drop(ws); // explicit application refusal BEFORE Connected.
        println!(
            "PASS response_extension_policy: library validated 101/Accept then explicit contains_key refusal before Connected"
        );
        let mut bad_accept = response("");
        let pos = bad_accept
            .windows(28)
            .position(|b| b == b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
            .unwrap();
        bad_accept[pos] = b'x';
        assert!(
            tungstenite::client(fixed_request(), stage_response(&bad_accept).unwrap()).is_err()
        );
        let redirect = b"HTTP/1.1 302 Found\r\nLocation: wss://localhost/elsewhere\r\n\r\n";
        assert!(tungstenite::client(fixed_request(), stage_response(redirect).unwrap()).is_err());
        assert_eq!(
            stage_response(&[b'a'; 8193]).err(),
            Some("HTTP header >8192")
        );
        let mut huge = vec![b'a'; 8190];
        huge.extend_from_slice(b"\r\n\r\n");
        assert_eq!(stage_response(&huge).err(), Some("HTTP header >8192"));
        println!(
            "PASS HTTP_library_refusals: invalid Accept and redirect rejected by library; stage header cap rejects complete >8192"
        );
        // Writer.flush alone cannot establish physical output completion.
        let before = Vec::<u8>::new().len();
        client.writer().write_all(b"synthetic-pending").unwrap();
        assert!(client.wants_write());
        client.writer().flush().unwrap();
        assert!(client.wants_write());
        let mut client_wire = Vec::new();
        assert_eq!(client_wire.len(), before);
        let mut partial = PartialSink {
            allow: 3,
            ..Default::default()
        };
        assert_eq!(client.write_tls(&mut partial).unwrap(), 3);
        assert!(client.wants_write());
        assert_eq!(
            client.write_tls(&mut partial).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        partial.allow = 65536;
        while client.wants_write() {
            client.write_tls(&mut partial).unwrap();
        }
        partial.flush().unwrap();
        client_wire.extend_from_slice(&partial.wire);
        deliver_to_server(&mut server, &client_wire);
        let mut received = [0; 64];
        let n = server.reader().read(&mut received).unwrap();
        assert_eq!(&received[..n], b"synthetic-pending");
        assert!(!client.wants_write());
        println!(
            "PASS rustls_writer_flush: plaintext Writer.flush left wants_write=true and 0 socket bytes; physical write_tls partial=3 then WouldBlock; explicit SAME TLS queue drain completed={} bytes, peer plaintext exact, residual wants_write=false",
            client_wire.len()
        );
        // Native Ping delivered encrypted: reading does not generate a TLS write or Pong.
        server.writer().write_all(&[0x89, 1, b'p']).unwrap();
        let mut ping_wire = Vec::new();
        drain_server(
            &mut server,
            &mut ping_wire,
            Instant::now() + Duration::from_secs(1),
        );
        deliver_to_client(&mut client, &ping_wire);
        assert!(!client.wants_write());
        let n = client.reader().read(&mut readbuf).unwrap();
        let mut raw = FrameSocket::new(Cursor::new(readbuf[..n].to_vec()));
        assert_eq!(
            raw.read(Some(65536)).unwrap().unwrap().header().opcode,
            OpCode::Control(Control::Ping)
        );
        assert!(!client.wants_write());
        println!(
            "PASS native_Ping_over_TLS: passive read_tls/process/reader/FrameSocket, socket writes=0, wants_write=false, no automatic Pong"
        );
        let _ = tls_pair(&rustls::version::TLS12);
    }

    pub(super) fn run() {
        println!(
            "SYNTHETIC IMPLEMENTATION-INTEGRATED TRANSPORT GATE ONLY; exact locked dependencies; no capture/WAL/replay acceptance; TLS loopback uses actual rustls records with offline localhost trust, HTTP coalescing record crosses numeric TCP127.0.0.1; live numeric endpoint is separate probe"
        );
        masks();
        partial_write();
        raw_passive_and_refusals();
        oversize_before_payload_reserve();
        absolute_deadlines();
        real_slow_drip_read_deadline();
        http_and_tls_coalescing();
        println!(
            "TRANSPORT_PROBE_PASS_SCOPED; full retained application/certificate allocation envelope remains independent gate"
        );
    }
}
mod allocation_probes {
    // Synthetic local fixtures, exact unmodified crates; instrumentation is probe only.
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
    use tungstenite::protocol::frame::FrameSocket;

    struct Tracer;
    #[global_allocator]
    static ALLOCATOR: Tracer = Tracer;
    static ACTIVE: AtomicBool = AtomicBool::new(false);
    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    static REQUEST_PEAK: AtomicUsize = AtomicUsize::new(0);
    static REQUESTS: AtomicUsize = AtomicUsize::new(0);
    static REQUEST_BYTES: AtomicUsize = AtomicUsize::new(0);
    static CAP: AtomicUsize = AtomicUsize::new(usize::MAX);
    static DENIED: AtomicUsize = AtomicUsize::new(0);
    static TRACE_N: AtomicUsize = AtomicUsize::new(0);
    // 2048 rows x 5 words = 81920 static bytes; no allocator recursion.
    static TRACE: [[AtomicUsize; 5]; 2048] = [const { [const { AtomicUsize::new(0) }; 5] }; 2048];
    unsafe extern "C" {
        fn write(fd: i32, buf: *const u8, count: usize) -> isize;
    }

    fn fatal_record(old: usize, size: usize, projected: usize, cap: usize) {
        let mut buf = [0u8; 200];
        let mut n = 0;
        for (label, value) in [
            (b"DENY before System old=".as_slice(), old),
            (b" request=", size),
            (b" request_live_envelope=", projected),
            (b" cap=", cap),
        ] {
            buf[n..n + label.len()].copy_from_slice(label);
            n += label.len();
            let mut digits = [0u8; 24];
            let mut d = 24;
            let mut v = value;
            loop {
                d -= 1;
                digits[d] = b'0' + (v % 10) as u8;
                v /= 10;
                if v == 0 {
                    break;
                }
            }
            buf[n..n + 24 - d].copy_from_slice(&digits[d..]);
            n += 24 - d;
        }
        buf[n] = b'\n';
        n += 1;
        // Direct fixed-stack libc write. Returning null causes Rust OOM abort; not graceful admission.
        unsafe {
            let _ = write(2, buf.as_ptr(), n);
        }
    }

    fn request(kind: usize, old: usize, size: usize) -> bool {
        super::transport_probes::watch_request(size);
        let live = LIVE.load(SeqCst);
        let projected = live.saturating_sub(old).saturating_add(size);
        // realloc may allocate a new block before freeing old: conservatively reserve both.
        let request_envelope = live.saturating_add(size);
        if ACTIVE.load(SeqCst) {
            REQUESTS.fetch_add(1, SeqCst);
            REQUEST_BYTES.fetch_add(size, SeqCst);
            REQUEST_PEAK.fetch_max(request_envelope, SeqCst);
            if size >= 4096 {
                let i = TRACE_N.fetch_add(1, SeqCst);
                if i < TRACE.len() {
                    for (dst, value) in
                        TRACE[i]
                            .iter()
                            .zip([kind, old, size, projected, request_envelope])
                    {
                        dst.store(value, SeqCst);
                    }
                }
            }
            let cap = CAP.load(SeqCst);
            if request_envelope > cap {
                DENIED.fetch_add(1, SeqCst);
                fatal_record(old, size, request_envelope, cap);
                return false;
            }
        }
        true
    }
    fn admitted(old: usize, size: usize) {
        let next = LIVE.fetch_sub(old, SeqCst) - old + size;
        LIVE.fetch_add(size, SeqCst);
        if ACTIVE.load(SeqCst) {
            PEAK.fetch_max(next, SeqCst);
        }
    }
    unsafe impl GlobalAlloc for Tracer {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if !request(1, 0, layout.size()) {
                return std::ptr::null_mut();
            }
            let p = unsafe { System.alloc(layout) };
            if !p.is_null() {
                admitted(0, layout.size());
            }
            p
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if !request(2, 0, layout.size()) {
                return std::ptr::null_mut();
            }
            let p = unsafe { System.alloc_zeroed(layout) };
            if !p.is_null() {
                admitted(0, layout.size());
            }
            p
        }
        unsafe fn realloc(&self, p: *mut u8, old: Layout, size: usize) -> *mut u8 {
            if !request(3, old.size(), size) {
                return std::ptr::null_mut();
            }
            let p = unsafe { System.realloc(p, old, size) };
            if !p.is_null() {
                admitted(old.size(), size);
            }
            p
        }
        unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
            LIVE.fetch_sub(layout.size(), SeqCst);
            unsafe {
                System.dealloc(p, layout);
            }
        }
    }
    fn begin(cap: usize) -> usize {
        let base = LIVE.load(SeqCst);
        PEAK.store(base, SeqCst);
        REQUEST_PEAK.store(base, SeqCst);
        REQUESTS.store(0, SeqCst);
        REQUEST_BYTES.store(0, SeqCst);
        TRACE_N.store(0, SeqCst);
        DENIED.store(0, SeqCst);
        CAP.store(cap, SeqCst);
        ACTIVE.store(true, SeqCst);
        base
    }
    fn end(name: &str, base: usize) {
        ACTIVE.store(false, SeqCst);
        println!(
            "CASE={name} baseline_live={base} final_live={} peak_live={} conservative_request_peak={} requests={} requested_bytes={} denied={} trace_count={}",
            LIVE.load(SeqCst),
            PEAK.load(SeqCst),
            REQUEST_PEAK.load(SeqCst),
            REQUESTS.load(SeqCst),
            REQUEST_BYTES.load(SeqCst),
            DENIED.load(SeqCst),
            TRACE_N.load(SeqCst)
        );
        for row in TRACE.iter().take(TRACE_N.load(SeqCst).min(TRACE.len())) {
            let v = row.each_ref().map(|x| x.load(SeqCst));
            println!(
                "TRACE case={name} op={} old={} requested={} projected_live={} conservative_request_live={}",
                v[0], v[1], v[2], v[3], v[4]
            );
        }
    }
    fn frame_cases() {
        let b = begin(usize::MAX);
        let frame = FrameSocket::new(Cursor::new(&[] as &[u8]));
        let (_, backing) = frame.into_inner();
        let capacity = backing.capacity();
        end("framesocket_new", b);
        println!("framesocket capacity={capacity} len={}", backing.len());
        assert_eq!(capacity, 131072);
        drop(backing);
        let advertised = 1u64 << 40;
        let mut hdr = [0u8; 10];
        hdr[0] = 0x81;
        hdr[1] = 127;
        hdr[2..].copy_from_slice(&advertised.to_be_bytes());
        let mut frame = FrameSocket::new(Cursor::new(hdr));
        let b = begin(usize::MAX);
        let result = frame.read(Some(65536));
        end("oversized_advertised", b);
        println!("oversized result={result:?}");
        assert!(result.is_err());
        drop(result);
        drop(frame);
        let bytes = [0x81, 1, b'a'];
        let mut frame = FrameSocket::new(Cursor::new(bytes));
        let b = begin(usize::MAX);
        let payload = frame.read(Some(65536)).unwrap().unwrap().into_payload();
        let payload_clone = payload.clone();
        let (_, remainder) = frame.into_inner();
        let remainder_capacity = remainder.capacity();
        drop(remainder);
        end("one_byte_retained_bytes", b);
        println!(
            "retained payload={} clone={} remainder_capacity={remainder_capacity} live_after_socket_drop={}",
            payload.len(),
            payload_clone.len(),
            LIVE.load(SeqCst)
        );
        drop(payload);
        println!("live_with_clone={}", LIVE.load(SeqCst));
        drop(payload_clone);
        println!("live_after_payload_clones_drop={}", LIVE.load(SeqCst));
    }
    fn u24(dst: &mut Vec<u8>, n: usize) {
        dst.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8]);
    }
    fn handshake(typ: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![typ];
        u24(&mut v, body.len());
        v.extend_from_slice(body);
        v
    }
    fn record(content: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![content, 3, 3, (body.len() >> 8) as u8, body.len() as u8];
        v.extend_from_slice(body);
        v
    }
    fn client(version: &'static rustls::SupportedProtocolVersion) -> rustls::ClientConnection {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut conf = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[version])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        conf.resumption = rustls::client::Resumption::disabled();
        conf.enable_early_data = false;
        let mut c = rustls::ClientConnection::new(
            Arc::new(conf),
            ServerName::try_from("synthetic.invalid").unwrap(),
        )
        .unwrap();
        c.set_buffer_limit(Some(65536));
        let mut hello = Vec::new();
        c.write_tls(&mut hello).unwrap();
        c
    }
    fn feed(
        c: &mut rustls::ClientConnection,
        wire: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut cursor = Cursor::new(wire);
        while cursor.position() < wire.len() as u64 {
            c.read_tls(&mut cursor)?;
            c.process_new_packets()?;
        }
        Ok(())
    }
    fn tls12_many_empty(n: usize, frag: usize, deny: bool) {
        let mut c = client(&rustls::version::TLS12);
        let mut sh = vec![3, 3];
        sh.extend_from_slice(&[0u8; 32]);
        sh.extend_from_slice(&[0, 0xc0, 0x2b, 0, 0, 0]);
        feed(&mut c, &record(22, &handshake(2, &sh))).unwrap();
        let mut body = Vec::with_capacity(3 + n * 3);
        u24(&mut body, n * 3);
        body.resize(3 + n * 3, 0);
        let hs = handshake(11, &body);
        let mut wire = Vec::new();
        for chunk in hs.chunks(frag) {
            wire.extend_from_slice(&record(22, chunk));
        }
        println!(
            "fixture tls12 empty_der_count={n} body={} handshake={} wire={} fragment={frag}",
            body.len(),
            hs.len(),
            wire.len()
        );
        let name = if deny {
            "tls12_deny_1m"
        } else {
            "tls12_many_empty"
        };
        let b = begin(if deny {
            LIVE.load(SeqCst) + 1048576
        } else {
            usize::MAX
        });
        let result = feed(&mut c, &wire);
        end(name, b);
        println!(
            "tls12 result={result:?} handshaking={} wants_write={}",
            c.is_handshaking(),
            c.wants_write()
        );
    }
    #[derive(Debug)]
    struct Resolve(Arc<rustls::sign::CertifiedKey>);
    impl rustls::server::ResolvesServerCert for Resolve {
        fn resolve(
            &self,
            _: rustls::server::ClientHello<'_>,
        ) -> Option<Arc<rustls::sign::CertifiedKey>> {
            Some(self.0.clone())
        }
    }
    fn tls13_many_empty(n: usize, ocsp: usize, frag: usize, deny: bool) {
        // Public server APIs generate authenticated encrypted TLS records, invalid certificate bodies.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut conf = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        conf.resumption = rustls::client::Resumption::disabled();
        conf.enable_early_data = false;
        let mut c = rustls::ClientConnection::new(
            Arc::new(conf),
            ServerName::try_from("synthetic.invalid").unwrap(),
        )
        .unwrap();
        c.set_buffer_limit(Some(65536));
        let mut hello = Vec::new();
        c.write_tls(&mut hello).unwrap();
        let keybytes = super::KEY.to_vec();
        let key = PrivateKeyDer::try_from(keybytes).unwrap();
        let key = provider.key_provider.load_private_key(key).unwrap();
        let certs: Vec<_> = (0..n).map(|_| CertificateDer::from(Vec::new())).collect();
        let mut certified = rustls::sign::CertifiedKey::new(certs, key);
        if ocsp > 0 {
            certified.ocsp = Some(vec![0u8; ocsp]);
        }
        let mut config = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(Resolve(Arc::new(certified))));
        config.max_fragment_size = Some(frag);
        config.send_tls13_tickets = 0;
        let mut s = rustls::ServerConnection::new(Arc::new(config)).unwrap();
        s.read_tls(&mut Cursor::new(hello)).unwrap();
        s.process_new_packets().unwrap();
        let mut wire = Vec::new();
        while s.wants_write() {
            s.write_tls(&mut wire).unwrap();
        }
        println!(
            "fixture tls13 empty_der_count={n} ocsp={ocsp} encrypted_flight={} fragment={frag}",
            wire.len()
        );
        // Source fixture allocations/server encoder are retained and accounted in absolute LIVE.
        // Their size is exposed by baseline; they are NOT production-client allocations.
        drop(s);
        let name = if deny {
            "tls13_deny_1m"
        } else if ocsp > 0 {
            "tls13_many_empty_ocsp"
        } else {
            "tls13_many_empty"
        };
        let b = begin(if deny {
            LIVE.load(SeqCst) + 1048576
        } else {
            usize::MAX
        });
        let result = feed(&mut c, &wire);
        end(name, b);
        println!(
            "tls13 result={result:?} handshaking={} wants_write={} parser_peak_delta={}",
            c.is_handshaking(),
            c.wants_write(),
            PEAK.load(SeqCst).saturating_sub(b)
        );
    }
    fn budget8m_refusal() {
        let base = LIVE.load(SeqCst);
        let cap = 8 * 1024 * 1024;
        let b = begin(cap);
        let mut buffer = Vec::<u8>::new();
        let result = buffer.try_reserve_exact(8 * 1024 * 1024 + 1);
        let capacity = buffer.capacity();
        let final_live_before_reporting = LIVE.load(SeqCst);
        end("synthetic_8m_guard_calibration", b);
        assert!(result.is_err());
        assert_eq!(capacity, 0);
        assert_eq!(DENIED.load(SeqCst), 1);
        assert_eq!(final_live_before_reporting, base);
        println!(
            "PASS synthetic_8m_guard_calibration: absolute_cap={cap}, baseline={base}, request=8388609 > remaining={}, before System allocator, try_reserve returned error, capacity=0, heap unchanged; not whole application composition proof",
            cap - base
        );
    }
    pub(super) fn run(mode: &str) {
        match mode {
            "frame" => frame_cases(),
            "budget8m-refusal" => budget8m_refusal(),
            "tls12" => tls12_many_empty(21800, 16384, false),
            "tls12-fragment" => tls12_many_empty(20000, 64, false),
            "tls12-deny" => tls12_many_empty(20000, 64, true),
            "tls13" => tls13_many_empty(12800, 0, 16384, false),
            "tls13-fragment" => tls13_many_empty(9000, 0, 64, false),
            "tls13-ocsp" => tls13_many_empty(9000, 300, 64, false),
            "tls13-deny" => tls13_many_empty(9000, 0, 64, true),
            _ => panic!("unknown mode"),
        }
    }
}
mod live_probe {
    use rustls::{ClientConfig, ClientConnection, RootCertStore};
    use std::{
        io::{self, Cursor, Read, Write},
        net::{SocketAddr, TcpStream},
        sync::Arc,
        time::{Duration, Instant},
    };
    use tungstenite::{
        client::IntoClientRequest,
        protocol::frame::{
            FrameSocket,
            coding::{Data, OpCode},
        },
    };
    fn timeout() -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, "original deadline expired")
    }
    fn transient(e: &io::Error) -> bool {
        matches!(
            e.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
        )
    }
    struct Limited<'a>(&'a mut TcpStream);
    impl Read for Limited<'_> {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            let n = b.len().min(4096);
            self.0.read(&mut b[..n])
        }
    }
    struct TlsIo {
        socket: TcpStream,
        tls: ClientConnection,
        prefix: Cursor<Vec<u8>>,
        deadline: Instant,
        read_progress: Option<Instant>,
        writes: usize,
        reads: usize,
    }
    impl TlsIo {
        fn check(&self) -> io::Result<()> {
            if Instant::now() >= self.deadline {
                Err(timeout())
            } else {
                Ok(())
            }
        }
        fn drain(&mut self) -> io::Result<()> {
            while self.tls.wants_write() {
                self.check()?;
                match self.tls.write_tls(&mut self.socket) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(n) => {
                        self.writes += n;
                    }
                    Err(e) if transient(&e) => continue,
                    Err(e) => return Err(e),
                }
            }
            self.check()?;
            self.socket.flush()
        }
        fn rx(&mut self) -> io::Result<()> {
            self.check()?;
            let n = self.tls.read_tls(&mut Limited(&mut self.socket))?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            self.reads += n;
            let was_handshaking = self.tls.is_handshaking();
            self.tls.process_new_packets().map_err(io::Error::other)?;
            if !was_handshaking && self.tls.wants_write() {
                return Err(io::Error::other("post-handshake wants_write fail-stop"));
            }
            Ok(())
        }
    }
    impl Read for TlsIo {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.check()?;
            let cap = b.len().min(4096);
            let n = self.prefix.read(&mut b[..cap])?;
            if n > 0 {
                return Ok(n);
            }
            loop {
                self.check()?;
                match self.tls.reader().read(&mut b[..cap]) {
                    Ok(n) if n > 0 => {
                        self.read_progress.get_or_insert(Instant::now());
                        return Ok(n);
                    }
                    Ok(_) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Err(e) if transient(&e) => {}
                    Err(e) => return Err(e),
                }
                self.rx()?;
            }
        }
    }
    impl Write for TlsIo {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.check()?;
            self.tls.writer().write(b)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.drain()
        }
    }
    struct Validation {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }
    impl Read for Validation {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.input.read(b)
        }
    }
    impl Write for Validation {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            if self.output.len() + b.len() > 8192 {
                return Err(io::Error::other("request cap"));
            }
            self.output.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn run() -> Result<(), Box<dyn std::error::Error>> {
        let address: SocketAddr = std::env::var("GATE45_NUMERIC_ADDRESS")?.parse()?;
        let begun = Instant::now();
        let total = begun + Duration::from_secs(7);
        let socket = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        socket.set_read_timeout(Some(Duration::from_millis(50)))?;
        socket.set_write_timeout(Some(Duration::from_millis(50)))?;
        println!(
            "TCP numeric={} elapsed_ms={}",
            address,
            begun.elapsed().as_millis()
        );
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut config = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.resumption = rustls::client::Resumption::disabled();
        config.enable_early_data = false;
        let mut tls = ClientConnection::new(Arc::new(config), "ws.bitget.com".try_into()?)?;
        tls.set_buffer_limit(Some(65536));
        let mut io = TlsIo {
            socket,
            tls,
            prefix: Cursor::new(vec![]),
            deadline: (Instant::now() + Duration::from_secs(3)).min(total),
            read_progress: None,
            writes: 0,
            reads: 0,
        };
        while io.tls.is_handshaking() {
            io.drain()?;
            if !io.tls.is_handshaking() {
                break;
            }
            match io.rx() {
                Ok(()) => {}
                Err(e) if transient(&e) => {
                    io.check()?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        let post = io.tls.wants_write();
        io.drain()?;
        println!(
            "TLS certificate_verified=true SNI=ws.bitget.com final_wants_write={} drained=true elapsed_ms={} tx={} rx={}",
            post,
            begun.elapsed().as_millis(),
            io.writes,
            io.reads
        );
        io.deadline = (Instant::now() + Duration::from_secs(2)).min(total);
        let request = "wss://ws.bitget.com/v3/ws/public".into_client_request()?;
        let (wire, _) = tungstenite::handshake::client::generate_request(request.clone())?;
        if wire.len() > 8192 {
            return Err("HTTP request >8192".into());
        }
        io.write_all(&wire)?;
        io.flush()?;
        let mut stage = Vec::with_capacity(12288);
        let mut chunk = [0u8; 4096];
        let split = loop {
            io.check()?;
            let n = match io.read(&mut chunk) {
                Ok(n) => n,
                Err(e) if transient(&e) => continue,
                Err(e) => return Err(e.into()),
            };
            stage.extend_from_slice(&chunk[..n]);
            if let Some(p) = stage.windows(4).position(|x| x == b"\r\n\r\n") {
                let end = p + 4;
                if end > 8192 {
                    return Err("HTTP response >8192".into());
                }
                break end;
            }
            if stage.len() >= 8192 {
                return Err("HTTP response header limit".into());
            }
        };
        let tail = stage.split_off(split);
        let header = stage;
        let input = Validation {
            input: Cursor::new(header),
            output: Vec::with_capacity(wire.len()),
        };
        let (ws, response) = tungstenite::client(request, input)
            .map_err(|e| format!("library HTTP validation: {e}"))?;
        if response.headers().contains_key("sec-websocket-extensions") {
            return Err("extensions refused after library validation".into());
        }
        let v = ws.into_inner();
        if v.output != wire {
            return Err("library request mismatch".into());
        }
        println!(
            "HTTP status={} validated=true header_bytes={} prefetched_tail={} elapsed_ms={}",
            response.status(),
            split,
            tail.len(),
            begun.elapsed().as_millis()
        );
        io.prefix = Cursor::new(tail);
        io.read_progress = None;
        io.deadline = Instant::now() + Duration::from_secs(1);
        let payload = r#"{"op":"subscribe","args":[{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"}]}"#;
        let deadline = io.deadline;
        let mut frames = FrameSocket::new(io);
        crate::transport::send_text_once(
            &mut frames,
            payload.as_bytes(),
            &provider,
            deadline,
            &std::sync::atomic::AtomicBool::new(false),
        )?;
        println!(
            "SUBSCRIBE enqueue=1 physical_TLS_drain=true bytes={} elapsed_ms={}",
            payload.len(),
            begun.elapsed().as_millis()
        );
        let tx_before_read = frames.get_ref().writes;
        frames.get_mut().deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match frames.read(Some(65536)) {
                Ok(Some(f)) => {
                    let h = f.header();
                    if !h.is_final
                        || h.rsv1
                        || h.rsv2
                        || h.rsv3
                        || h.mask.is_some()
                        || h.opcode != OpCode::Data(Data::Text)
                    {
                        return Err(format!("unsupported header: {h:?}").into());
                    }
                    std::str::from_utf8(f.payload())?;
                    std::fs::write(
                        "/evidence/live/endpoint-first-frame.private.bin",
                        f.payload(),
                    )?;
                    assert_eq!(frames.get_ref().writes, tx_before_read);
                    println!(
                        "FIRST_FRAME valid_UTF8_Text=true length={} read_phase_tx_delta={} elapsed_ms={}",
                        f.payload().len(),
                        frames.get_ref().writes - tx_before_read,
                        begun.elapsed().as_millis()
                    );
                    return Ok(());
                }
                Ok(None) => return Err("EOF".into()),
                Err(tungstenite::Error::Io(e)) if transient(&e) => {
                    frames.get_ref().check()?;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    pub(super) fn entry() {
        match run() {
            Ok(()) => println!("ENDPOINT_ROUTE_PASS_SCOPED; no WAL/capture acceptance"),
            Err(e) => {
                eprintln!("ENDPOINT_ROUTE_BLOCKED: {e}");
                std::process::exit(1)
            }
        }
    }
}

pub fn run() {
    let mode = std::env::var("GATE45_PROBE_MODE").unwrap_or_else(|_| "offline".into());
    match mode.as_str() {
        "transport" => transport_probes::run(),
        "live" => live_probe::entry(),
        "offline" => {
            helper_dispatch_faults();
            transport_probes::run();
            for mode in [
                "frame",
                "budget8m-refusal",
                "tls12",
                "tls12-fragment",
                "tls13",
                "tls13-fragment",
                "tls13-ocsp",
            ] {
                allocation_probes::run(mode);
            }
            for mode in ["tls12-deny", "tls13-deny"] {
                use std::os::unix::process::ExitStatusExt;
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "implementation_integrated_probes",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("GATE45_PROBE_MODE", mode)
                    .output()
                    .unwrap();
                assert!(output.stdout.len() + output.stderr.len() <= 131072);
                println!(
                    "CHILD mode={mode} status={} signal={:?}\n{}\n{}",
                    output.status,
                    output.status.signal(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(output.status.signal(), Some(6));
                assert!(String::from_utf8_lossy(&output.stderr).contains("DENY before System"));
                println!("EXPECTED_KILLED_UNKNOWN_INCOMPLETE: {mode}; NOT production budget PASS");
            }
        }
        other => allocation_probes::run(other),
    }
}

// Tests the actual implementation helper. No owner/lease authority is fabricated.
fn helper_dispatch_faults() {
    use std::io::{self, Write};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    use tungstenite::protocol::frame::FrameSocket;
    struct Sink<'a> {
        wire: Vec<u8>,
        calls: usize,
        flushes: usize,
        stop: &'a AtomicBool,
        cancel: bool,
    }
    impl Write for Sink<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls == 2 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let n = if self.calls == 1 {
                bytes.len().min(3)
            } else {
                bytes.len()
            };
            self.wire.extend_from_slice(&bytes[..n]);
            if self.cancel {
                self.stop.store(true, Ordering::SeqCst);
            }
            Ok(n)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            if self.flushes == 1 {
                Err(io::ErrorKind::WouldBlock.into())
            } else {
                Ok(())
            }
        }
    }
    let provider = rustls::crypto::ring::default_provider();
    for cancel in [false, true] {
        let stop = AtomicBool::new(false);
        let mut socket = FrameSocket::new(Sink {
            wire: Vec::new(),
            calls: 0,
            flushes: 0,
            stop: &stop,
            cancel,
        });
        let result = crate::transport::send_text_once(
            &mut socket,
            b"once",
            &provider,
            Instant::now() + Duration::from_secs(1),
            &stop,
        );
        if cancel {
            assert!(
                matches!(result, Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::Interrupted)
            );
            assert_eq!(socket.get_ref().wire.len(), 3);
            assert_eq!(socket.get_ref().flushes, 0);
        } else {
            result.unwrap();
            assert_eq!(socket.get_ref().wire.len(), 10);
            assert_eq!(socket.get_ref().flushes, 2);
            let mut decoder = FrameSocket::new(io::Cursor::new(&socket.get_ref().wire));
            let frame = decoder.read(Some(65536)).unwrap().unwrap();
            let mask = frame.header().mask.unwrap();
            for (index, expected) in b"once".iter().enumerate() {
                assert_eq!(frame.payload()[index] ^ mask[index % 4], *expected);
            }
            assert!(decoder.read(Some(65536)).unwrap().is_none());
        }
    }
    let stop = AtomicBool::new(false);
    let mut socket = FrameSocket::new(Vec::new());
    assert!(
        crate::transport::send_text_once(&mut socket, b"expired", &provider, Instant::now(), &stop)
            .is_err()
    );
    assert!(socket.get_ref().is_empty());
    assert!(crate::transport::masked_text(&[0u8; 4097], &provider).is_err());
    println!(
        "PASS actual_send_helper: one serialized frame after partial/WouldBlock/flush; atomic cancel after 3 bytes -> Interrupted/ambiguous prefix; expired original deadline -> 0 socket bytes; payload4097 refused"
    );
}
