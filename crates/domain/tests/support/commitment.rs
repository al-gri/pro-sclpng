//! PSCO whole-frame output commitment bytes. No SHA computation occurs here.

use domain::event::{EventError, Level, LevelChange, MAX_BOOK_ENTRIES, MarketPayload, Side};
use domain::numeric::{PriceTicks, QuantitySteps};

use super::binary::{Error, ErrorKind, Reader, Result, Writer, checked};

fn side(r: &mut Reader<'_>) -> Result<Side> {
    let offset = r.offset();
    let value = r.u8()?;
    match value {
        1 => Ok(Side::Bid),
        2 => Ok(Side::Ask),
        _ => Err(Error::new(
            offset,
            ErrorKind::Unsupported {
                field: "Side",
                value: value.into(),
            },
        )),
    }
}

fn price(r: &mut Reader<'_>) -> Result<PriceTicks> {
    let offset = r.offset();
    checked(offset, PriceTicks::new(r.u64()?))
}

fn homogeneous(outputs: &[MarketPayload]) -> Result<()> {
    if outputs.is_empty() || outputs.len() > 65_536 {
        return Err(Error::new(6, ErrorKind::LengthError));
    }
    if outputs.len() == 1 && matches!(outputs[0], MarketPayload::Snapshot(_)) {
        return checked(10, outputs[0].validate());
    }
    for output in outputs {
        if !matches!(output, MarketPayload::Update(_)) {
            return Err(Error::new(
                10,
                ErrorKind::Event(EventError::MixedFrameUnsupported),
            ));
        }
        checked(10, output.validate())?;
    }
    Ok(())
}

pub fn decode_commitment(bytes: &[u8]) -> Result<Vec<MarketPayload>> {
    let mut r = Reader::new(bytes, 0)?;
    if r.take(4)? != b"PSCO" {
        return Err(Error::new(0, ErrorKind::Corrupt("PSCO.magic")));
    }
    if r.u16()? != 1 {
        return Err(Error::new(
            4,
            ErrorKind::Event(EventError::UnsupportedSchema),
        ));
    }
    let count = usize::try_from(r.u32()?).map_err(|_| Error::new(6, ErrorKind::LengthError))?;
    r.count(count, 5, 65_536)?;
    let mut outputs = Vec::with_capacity(count);
    for _ in 0..count {
        let offset = r.offset();
        let tag = r.u8()?;
        let entries = usize::try_from(r.u32()?)
            .map_err(|_| Error::new(offset + 1, ErrorKind::LengthError))?;
        let output = match tag {
            1 => {
                r.count(entries, 17, MAX_BOOK_ENTRIES)?;
                let mut levels = Vec::with_capacity(entries);
                for _ in 0..entries {
                    levels.push(Level {
                        side: side(&mut r)?,
                        price: price(&mut r)?,
                        quantity: QuantitySteps::new(r.u64()?),
                    });
                }
                MarketPayload::Snapshot(levels)
            }
            2 => {
                r.count(entries, 10, MAX_BOOK_ENTRIES)?;
                let mut changes = Vec::with_capacity(entries);
                for _ in 0..entries {
                    let offset = r.offset();
                    let operation = r.u8()?;
                    if operation != 1 && operation != 2 {
                        return Err(Error::new(
                            offset,
                            ErrorKind::Unsupported {
                                field: "PSCO.operation",
                                value: operation.into(),
                            },
                        ));
                    }
                    let side = side(&mut r)?;
                    let price = price(&mut r)?;
                    changes.push(if operation == 1 {
                        LevelChange::Set(Level {
                            side,
                            price,
                            quantity: QuantitySteps::new(r.u64()?),
                        })
                    } else {
                        LevelChange::Delete { side, price }
                    });
                }
                MarketPayload::Update(changes)
            }
            _ => {
                return Err(Error::new(
                    offset,
                    ErrorKind::Unsupported {
                        field: "PSCO.output_tag",
                        value: tag.into(),
                    },
                ));
            }
        };
        checked(offset, output.validate())?;
        outputs.push(output);
    }
    r.finish()?;
    homogeneous(&outputs)?;
    Ok(outputs)
}

pub fn encode_commitment(outputs: &[MarketPayload]) -> Result<Vec<u8>> {
    homogeneous(outputs)?;
    // A commitment cannot contain more than the validated source outputs and
    // 4096 entries per output. Checked writer capacity, not an untrusted reserve.
    let cap = outputs
        .len()
        .checked_mul(5 + MAX_BOOK_ENTRIES * 18)
        .and_then(|v| v.checked_add(10))
        .ok_or_else(|| Error::new(6, ErrorKind::LengthError))?;
    let mut w = Writer::new(cap);
    w.bytes(b"PSCO")?;
    w.u16(1)?;
    w.u32(u32::try_from(outputs.len()).expect("bounded output count"))?;
    for output in outputs {
        match output {
            MarketPayload::Snapshot(levels) => {
                w.u8(1)?;
                w.u32(u32::try_from(levels.len()).expect("validated entry cap"))?;
                for level in levels {
                    w.u8(level.side as u8)?;
                    w.u64(level.price.get())?;
                    w.u64(level.quantity.get())?;
                }
            }
            MarketPayload::Update(changes) => {
                w.u8(2)?;
                w.u32(u32::try_from(changes.len()).expect("validated entry cap"))?;
                for change in changes {
                    match change {
                        LevelChange::Set(level) => {
                            w.u8(1)?;
                            w.u8(level.side as u8)?;
                            w.u64(level.price.get())?;
                            w.u64(level.quantity.get())?;
                        }
                        LevelChange::Delete { side, price } => {
                            w.u8(2)?;
                            w.u8(*side as u8)?;
                            w.u64(price.get())?;
                        }
                    }
                }
            }
            MarketPayload::Trade(_) => unreachable!("homogeneous guard rejects trades"),
        }
    }
    Ok(w.finish())
}
