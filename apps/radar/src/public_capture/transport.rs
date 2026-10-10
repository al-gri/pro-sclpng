//! Unactivated #45 transport primitives. The memory-composition gate remains open.
//! No socket/owner/capture entry point is exposed by these experimental primitives.
use rustls::crypto::CryptoProvider;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tungstenite::protocol::frame::{
    Frame, FrameSocket,
    coding::{Data, OpCode},
};

pub const MAX_TEXT: usize = 65536;
pub const MAX_SEND: usize = 4096;

pub fn validate_text(frame: &Frame) -> Result<&str, &'static str> {
    let h = frame.header();
    if !h.is_final || h.rsv1 || h.rsv2 || h.rsv3 || h.mask.is_some() {
        return Err("unsupported frame header");
    }
    if h.opcode != OpCode::Data(Data::Text) {
        return Err("unsupported opcode; native controls fail-stop");
    }
    if frame.payload().len() > MAX_TEXT {
        return Err("payload limit");
    }
    std::str::from_utf8(frame.payload()).map_err(|_| "invalid UTF8")
}

/// Each caller must already own a counted lease and prepaid memory envelope.
/// This helper grants neither permission nor successful owner completion.
pub fn masked_text(payload: &[u8], provider: &CryptoProvider) -> io::Result<Frame> {
    if payload.len() > MAX_SEND {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(payload.len())
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    bytes.extend_from_slice(payload);
    let mut mask = [0; 4];
    provider
        .secure_random
        .fill(&mut mask)
        .map_err(|_| io::Error::other("mask entropy unavailable"))?;
    let mut frame = Frame::message(bytes, OpCode::Data(Data::Text), true);
    frame.header_mut().mask = Some(mask);
    Ok(frame)
}

fn transient(error: &tungstenite::Error) -> bool {
    matches!(error, tungstenite::Error::Io(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted))
}

fn check(original_deadline: Instant, stop: &AtomicBool) -> tungstenite::Result<()> {
    if stop.load(Ordering::SeqCst) {
        return Err(io::Error::from(io::ErrorKind::Interrupted).into());
    }
    if Instant::now() >= original_deadline {
        return Err(io::Error::from(io::ErrorKind::TimedOut).into());
    }
    Ok(())
}

/// Enqueue once, then flush that SAME queue. Every error remains an ambiguous
/// local effect for the future owner dispatch adapter; never resend its suffix.
pub fn send_text_once<S: Write>(
    socket: &mut FrameSocket<S>,
    payload: &[u8],
    provider: &CryptoProvider,
    original_deadline: Instant,
    stop: &AtomicBool,
) -> tungstenite::Result<()> {
    check(original_deadline, stop)?;
    let frame = masked_text(payload, provider)?;
    check(original_deadline, stop)?;
    // Exactly one enqueue, including when write has already emitted a prefix.
    match socket.write(frame) {
        Ok(()) => {}
        Err(error) if transient(&error) => {}
        Err(error) => return Err(error),
    }
    loop {
        check(original_deadline, stop)?;
        match socket.flush() {
            Ok(()) => {
                check(original_deadline, stop)?;
                return Ok(());
            }
            Err(error) if transient(&error) => {}
            Err(error) => return Err(error),
        }
    }
}
