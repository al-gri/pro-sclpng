//! Nondefault E1 author experiment. Untagged harness/std thread allocations are
//! unprotected; no baseline subtraction or whole-application/TLS proof claimed.
#[path = "support/alloc45_u4_core.rs"]
mod core;

use core::*;
use std::alloc::Layout;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[global_allocator]
static FAMILY: FamilyAllocator = FamilyAllocator;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn bootstrap(stop: &AtomicBool) -> (CoreStrong, CoreReclaimOwner) {
    set_null_fault(0);
    try_bootstrap(deadline(), stop).unwrap()
}
fn drain(owner: &mut CoreReclaimOwner) -> ReclaimReport {
    let report = collect_retired(owner, RECORDS);
    assert!(!report.busy);
    assert!(!report.arithmetic_blocked);
    assert_eq!(report.freed, report.debited);
    report
}
fn finish(core: CoreStrong, mut owner: CoreReclaimOwner, name: &str) {
    let report = drain(&mut owner);
    assert!(!report.complete);
    let s = owner.snapshot().unwrap();
    assert_eq!(s.states, [RECORDS, 0, 0, 0, 0]);
    assert_eq!(s.counts, bootstrap_layouts().unwrap().1);
    assert_eq!(s.aggregate, s.counts);
    assert_eq!(s.frees, s.debits);
    assert_eq!(s.retired, s.frees);
    println!("E1 {name}: {s:?}"); // outside any protected CallPlan/hook
    drop(core);
    assert!(drain(&mut owner).complete);
    assert_eq!(aggregate_snapshot().unwrap(), Counts::default());
}

fn bootstrap_and_layouts() {
    println!(
        "E1 actual fixture sizes Header/core/record/entry/book/custodian/trace/Strong/Weak/owner/Prepaid/CallPlan={:?}",
        fixture_sizes()
    );
    let stop = AtomicBool::new(false);
    for nth in 1..=3 {
        set_null_fault(nth);
        assert!(matches!(
            try_bootstrap(deadline(), &stop),
            Err(CoreFailure::AllocatorNull)
        ));
        assert_eq!(direct_counts(), (nth, nth - 1));
        assert_eq!(system_counts(), (nth - 1, nth - 1)); // injected null, not real OOM
        println!(
            "E1 bootstrap injected-null nth={nth} boundary={:?} actual-System={:?}",
            direct_counts(),
            system_counts()
        );
        assert_eq!(aggregate_snapshot().unwrap(), Counts::default());
    }
    set_null_fault(0);
    stop.store(true, Ordering::Release);
    assert!(matches!(
        try_bootstrap(deadline(), &stop),
        Err(CoreFailure::Stopped)
    ));
    stop.store(false, Ordering::Release);
    assert!(matches!(
        try_bootstrap(Instant::now(), &stop),
        Err(CoreFailure::DeadlineExpired)
    ));
    let busy = hold_admission().unwrap();
    assert!(matches!(
        try_bootstrap(deadline(), &stop),
        Err(CoreFailure::AdmissionBusy)
    ));
    drop(busy);
    assert_eq!(direct_counts(), (0, 0));
    let (core, owner) = bootstrap(&stop);
    let (layouts, counts) = bootstrap_layouts().unwrap();
    assert_eq!(
        counts.0[0],
        layouts.into_iter().map(|l| l.size()).sum::<usize>()
    );
    assert_eq!(counts.0[5], DIAGNOSTIC);
    assert_eq!(core.snapshot().unwrap().counts, counts);
    set_null_fault(0);
    // Second ledger's diagnostic backing would exceed aggregate Diagnostic.
    // No exempt bootstrap and no separately reset per-ledger total.
    assert!(matches!(
        try_bootstrap(deadline(), &stop),
        Err(CoreFailure::BufferQuota)
    ));
    assert_eq!(direct_counts(), (0, 0));
    let (physical, offset) = family_layout(Layout::new::<Pod>()).unwrap();
    assert_eq!(physical.align(), 128);
    assert_eq!(offset % 128, 0);
    assert_eq!(physical.size() % 128, 0);
    assert!(physical.size() >= offset + std::mem::size_of::<Pod>());
    println!(
        "E1 layouts bootstrap={layouts:?} counts={counts:?} Pod={} offset={offset} F={physical:?}",
        std::mem::size_of::<Pod>()
    );
    finish(core, owner, "bootstrap/layout/first-later-null/aggregate");
}

fn refusal_and_nulls() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let op = CoreOp {
        core: &core,
        deadline: deadline(),
        stop: &stop,
    };
    let before = core.snapshot().unwrap();
    set_null_fault(0);
    assert!(matches!(
        try_vec_bytes(&op, CoreClass::Handshake, H),
        Err(CoreFailure::HandshakeQuota)
    ));
    assert!(matches!(
        try_vec_bytes(&op, CoreClass::Configuration, T),
        Err(CoreFailure::TotalQuota)
    ));
    assert!(matches!(
        try_vec_bytes(&op, CoreClass::RxPlain, BUFFER),
        Err(CoreFailure::BufferQuota)
    ));
    assert!(matches!(
        try_vec_bytes(&op, CoreClass::TxPlain, BUFFER),
        Err(CoreFailure::BufferQuota)
    ));
    assert!(matches!(
        try_vec_bytes(&op, CoreClass::TxRecord, BUFFER),
        Err(CoreFailure::BufferQuota)
    ));
    assert!(matches!(
        try_vec_bytes(&op, CoreClass::Diagnostic, 1),
        Err(CoreFailure::BufferQuota)
    ));
    assert!(matches!(
        try_vec_bytes(&op, CoreClass::Ledger, usize::MAX),
        Err(CoreFailure::ArithmeticOverflow)
    ));
    let huge = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
    assert!(matches!(
        try_prepare(&op, CoreClass::Ledger, huge),
        Err(CoreFailure::ArithmeticOverflow)
    ));
    let busy = hold_admission().unwrap();
    assert!(matches!(
        try_prepaid_pod_box(&op, CoreClass::Ledger, Pod::new(1)),
        Err(CoreFailure::AdmissionBusy)
    ));
    let pending = collect_retired(&mut owner, RECORDS);
    assert!(pending.busy);
    assert!(!pending.complete);
    assert_eq!(pending.remaining, None);
    drop(busy);
    stop.store(true, Ordering::Release);
    assert!(matches!(
        try_prepaid_pod_box(&op, CoreClass::Ledger, Pod::new(1)),
        Err(CoreFailure::Stopped)
    ));
    assert!(matches!(
        core.try_share(op.deadline, &stop),
        Err(CoreFailure::Stopped)
    ));
    assert!(matches!(
        core.try_downgrade(op.deadline, &stop),
        Err(CoreFailure::Stopped)
    ));
    stop.store(false, Ordering::Release);
    let expired = CoreOp {
        deadline: Instant::now(),
        ..op
    };
    assert!(matches!(
        try_prepaid_pod_box(&expired, CoreClass::Ledger, Pod::new(1)),
        Err(CoreFailure::DeadlineExpired)
    ));
    assert_eq!(direct_counts(), (0, 0));
    let after = core.snapshot().unwrap();
    assert_eq!(before, after); // not just returned error: no mutation/calls
    assert_eq!(system_counts(), (0, 0));
    // SAFETY: sole driver/no concurrent users; each artificial count restored
    // before any handle Drop/reclamation. No extra strong references exist.
    unsafe { set_refcount_fault(&core, true, usize::MAX) };
    assert!(matches!(
        core.try_share(op.deadline, &stop),
        Err(CoreFailure::ReferenceCountOverflow)
    ));
    unsafe { set_refcount_fault(&core, true, 1) };
    unsafe { set_refcount_fault(&core, false, usize::MAX) };
    assert!(matches!(
        core.try_downgrade(op.deadline, &stop),
        Err(CoreFailure::ReferenceCountOverflow)
    ));
    unsafe { set_refcount_fault(&core, false, 0) };
    let weak = core.try_downgrade(op.deadline, &stop).unwrap();
    unsafe { set_refcount_fault(&core, true, usize::MAX) };
    assert!(matches!(
        weak.try_upgrade(op.deadline, &stop),
        Err(CoreFailure::ReferenceCountOverflow)
    ));
    unsafe { set_refcount_fault(&core, true, 1) };
    assert!(matches!(
        weak.try_upgrade(Instant::now(), &stop),
        Err(CoreFailure::DeadlineExpired)
    ));
    drop(weak);
    assert_eq!(direct_counts(), (0, 0));
    for kind in 0..3 {
        set_null_fault(1);
        match kind {
            0 => assert!(matches!(
                try_vec_bytes(&op, CoreClass::Handshake, 42),
                Err(CoreFailure::AllocatorNull)
            )),
            1 => assert!(matches!(
                try_string(&op, CoreClass::Handshake, "returned allocation error"),
                Err(CoreFailure::AllocatorNull)
            )),
            _ => assert!(matches!(
                try_prepaid_pod_box(&op, CoreClass::Handshake, Pod::new(7)),
                Err(CoreFailure::AllocatorNull)
            )),
        }
        assert_eq!(direct_counts(), (1, 0));
        assert_eq!(system_counts(), (0, 0)); // null fixture intercepted before real System
        let s = core.snapshot().unwrap();
        assert_eq!(s.counts, before.counts);
        assert_eq!((s.consumed, s.downstream), (0, 0));
        assert_eq!(s.states, [RECORDS, 0, 0, 0, 0]);
    }
    set_null_fault(0);
    finish(
        core,
        owner,
        "pre-admission-zero-calls/overflow/refs/null-rollback",
    );
}

fn exact_limits() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let op = CoreOp {
        core: &core,
        deadline: deadline(),
        stop: &stop,
    };
    let offset = family_layout(Layout::new::<u8>()).unwrap().1;
    for (class, limit, counter) in [
        (CoreClass::Handshake, H, 1),
        (CoreClass::RxPlain, BUFFER, 2),
        (CoreClass::TxPlain, BUFFER, 3),
        (CoreClass::TxRecord, BUFFER, 4),
        (CoreClass::HandshakeTxRecord, BUFFER, 4),
    ] {
        let mut bytes = try_vec_bytes(&op, class, limit - offset).unwrap();
        bytes.extend_from_slice(b"original");
        let s = core.snapshot().unwrap();
        assert_eq!(s.counts.0[counter], limit);
        if class == CoreClass::HandshakeTxRecord {
            assert_eq!(s.counts.0[1], limit);
        }
        assert_eq!(s.counts.0[0], bootstrap_layouts().unwrap().1.0[0] + limit); // mixed counted once
        let calls = s.prepare_calls;
        assert!(matches!(
            try_vec_bytes(&op, class, 1),
            Err(CoreFailure::HandshakeQuota | CoreFailure::BufferQuota)
        ));
        assert_eq!(core.snapshot().unwrap().prepare_calls, calls);
        let old = (bytes.as_ptr(), bytes.capacity());
        // Even shrink is old+new: exact old sublimit refuses BEFORE allocation.
        assert!(matches!(
            try_resize_bytes(&op, class, &mut bytes, 32),
            Err(CoreFailure::HandshakeQuota | CoreFailure::BufferQuota)
        ));
        assert_eq!((bytes.as_ptr(), bytes.capacity()), old);
        assert_eq!(bytes, b"original");
        assert_eq!(core.snapshot().unwrap().prepare_calls, calls);
        drop(bytes);
        assert_eq!(core.snapshot().unwrap().counts, s.counts); // retirement doesn't debit
        assert_eq!(drain(&mut owner).freed, 1);
    }
    let base = bootstrap_layouts().unwrap().1.0[0];
    let bytes = try_vec_bytes(&op, CoreClass::Configuration, T - base - offset).unwrap();
    assert_eq!(core.snapshot().unwrap().counts.0[0], T);
    let calls = core.snapshot().unwrap().prepare_calls;
    assert!(matches!(
        try_vec_bytes(&op, CoreClass::Ledger, 1),
        Err(CoreFailure::TotalQuota)
    ));
    assert_eq!(core.snapshot().unwrap().prepare_calls, calls);
    drop(bytes);
    assert_eq!(core.snapshot().unwrap().counts.0[0], T);
    drain(&mut owner);
    finish(core, owner, "exact-T-H-three-buffers/mixed/no-baseline");
}

fn finite_leaves_and_traces() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let op = CoreOp {
        core: &core,
        deadline: deadline(),
        stop: &stop,
    };
    let empty = try_vec_bytes(&op, CoreClass::Handshake, 0).unwrap();
    assert_eq!(empty.capacity(), 0);
    let empty_string = try_string(&op, CoreClass::Handshake, "").unwrap();
    assert_eq!(core.snapshot().unwrap().prepare_calls, 0);
    drop((empty, empty_string));
    let mut bytes = try_vec_bytes(&op, CoreClass::Handshake, 33).unwrap();
    bytes.extend_from_slice(b"compatible Vec initialized length");
    let text = try_string(&op, CoreClass::Handshake, "verified UTF-8: \u{03bb}").unwrap();
    let pod = try_prepaid_pod_box(&op, CoreClass::Handshake, Pod::new(23)).unwrap();
    assert_eq!(*pod, Pod::new(23));
    assert_eq!((pod.as_ref() as *const Pod as usize) % 128, 0);
    let s = core.snapshot().unwrap();
    assert_eq!(s.prepare_calls, 3);
    assert_eq!(s.consumed, 3); // actual pinned debug observation, not optimizer guarantee
    assert_eq!(s.downstream, 1);
    drop((bytes, text, pod));
    let retired = core.snapshot().unwrap();
    assert_eq!(retired.states[3], 3);
    assert_eq!(retired.counts, s.counts);
    unused_pod_ticket(&op, CoreClass::Handshake).unwrap();
    let unused = core.snapshot().unwrap();
    assert_eq!(unused.prepare_calls, 4);
    assert_eq!(unused.consumed, 3);
    assert_eq!(unused.states[3], 4); // safely unused ticket remains paid
    let calls = unused.prepare_calls;
    assert_eq!(nested_plan_stop(&op), Err(CoreFailure::UnresolvedClosure));
    let stopped = core.snapshot().unwrap();
    assert_eq!(stopped.prepare_calls, calls + 1); // outer ticket only; inner refused before alloc/Box
    assert_eq!(stopped.downstream, 1);
    drain(&mut owner);
    let mut previous = None;
    let count = owner.snapshot().unwrap().trace_len;
    for i in 0..count {
        let tr = owner.trace(i).unwrap().unwrap();
        if tr.kind == 6 {
            let before: Trace = previous.unwrap();
            assert_eq!(before.kind, 5);
            assert_eq!((tr.id, tr.generation), (before.id, before.generation));
            assert_eq!(before.total - tr.total, tr.bytes); // free RETURN precedes debit
        }
        println!("E1 trace {tr:?}");
        previous = Some(tr);
    }
    assert!(owner.trace(count).unwrap().is_none());
    assert_eq!(hook_counts(), (0, 0, 0));
    finish(
        core,
        owner,
        "Vec-String-Box/unused-ticket/pre-invocation-stop/free-before-debit",
    );
}

fn moving_resize() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let op = CoreOp {
        core: &core,
        deadline: deadline(),
        stop: &stop,
    };
    let base = bootstrap_layouts().unwrap().1.0[0];
    let mut bytes = try_vec_bytes(&op, CoreClass::Handshake, 128).unwrap();
    bytes.extend_from_slice(b"old bytes survive failure");
    let old_pointer = bytes.as_ptr();
    let old_capacity = bytes.capacity();
    let old_charge = family_layout(Layout::array::<u8>(128).unwrap())
        .unwrap()
        .0
        .size();
    let before = core.snapshot().unwrap();
    assert!(matches!(
        try_resize_bytes(&op, CoreClass::Handshake, &mut bytes, H),
        Err(CoreFailure::HandshakeQuota)
    ));
    assert_eq!(
        (bytes.as_ptr(), bytes.capacity()),
        (old_pointer, old_capacity)
    );
    assert_eq!(bytes, b"old bytes survive failure");
    assert_eq!(core.snapshot().unwrap(), before);
    set_null_fault(1);
    assert_eq!(
        try_resize_bytes(&op, CoreClass::Handshake, &mut bytes, 512),
        Err(CoreFailure::AllocatorNull)
    );
    assert_eq!(
        (bytes.as_ptr(), bytes.capacity()),
        (old_pointer, old_capacity)
    );
    assert_eq!(bytes, b"old bytes survive failure");
    set_null_fault(0);
    try_resize_bytes(&op, CoreClass::Handshake, &mut bytes, 512).unwrap();
    let grown = bytes.as_ptr();
    assert_ne!(grown, old_pointer);
    let new_charge = family_layout(Layout::array::<u8>(512).unwrap())
        .unwrap()
        .0
        .size();
    assert_eq!(
        core.snapshot().unwrap().counts.0[0],
        base + old_charge + new_charge
    );
    assert_eq!(core.snapshot().unwrap().states[3], 1);
    drain(&mut owner);
    assert_eq!(core.snapshot().unwrap().counts.0[0], base + new_charge);
    // Shrink null also preserves original pointer/capacity/content.
    set_null_fault(1);
    assert_eq!(
        try_resize_bytes(&op, CoreClass::Handshake, &mut bytes, 32),
        Err(CoreFailure::AllocatorNull)
    );
    assert_eq!((bytes.as_ptr(), bytes.capacity()), (grown, 512));
    assert_eq!(bytes, b"old bytes survive failure");
    set_null_fault(0);
    try_resize_bytes(&op, CoreClass::Handshake, &mut bytes, 32).unwrap();
    assert_ne!(bytes.as_ptr(), grown);
    let shrink_charge = family_layout(Layout::array::<u8>(32).unwrap())
        .unwrap()
        .0
        .size();
    assert_eq!(
        core.snapshot().unwrap().counts.0[0],
        base + new_charge + shrink_charge
    );
    assert_eq!(bytes, b"old bytes survive failure");
    assert_eq!(bytes.capacity(), 32);
    assert_eq!(
        try_resize_bytes(&op, CoreClass::Configuration, &mut bytes, 64),
        Err(CoreFailure::UnresolvedClosure)
    );
    assert_eq!(
        try_resize_bytes(&op, CoreClass::Handshake, &mut bytes, 1),
        Err(CoreFailure::UnresolvedClosure)
    );
    drop(bytes);
    finish(core, owner, "moving-grow-shrink/null-refusal-preserves-old");
}

fn records_and_generation() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let op = CoreOp {
        core: &core,
        deadline: deadline(),
        stop: &stop,
    };
    let mut tickets: [Option<Prepaid>; RECORDS] = std::array::from_fn(|_| None);
    for slot in &mut tickets {
        *slot = Some(try_prepare(&op, CoreClass::Ledger, Layout::new::<u64>()).unwrap());
    }
    let calls = core.snapshot().unwrap().prepare_calls;
    assert!(matches!(
        try_prepare(&op, CoreClass::Ledger, Layout::new::<u64>()),
        Err(CoreFailure::AdmissionBusy)
    ));
    assert_eq!(core.snapshot().unwrap().prepare_calls, calls);
    drop(tickets);
    assert_eq!(core.snapshot().unwrap().states[3], RECORDS);
    assert!(matches!(
        try_prepare(&op, CoreClass::Ledger, Layout::new::<u64>()),
        Err(CoreFailure::AdmissionBusy)
    ));
    assert_eq!(collect_retired(&mut owner, 0).freed, 0);
    for _ in 0..RECORDS {
        let r = collect_retired(&mut owner, 1);
        assert_eq!(r.scanned, 1);
        assert_eq!(r.freed, 1);
    }
    let ticket = try_prepare(&op, CoreClass::Ledger, Layout::new::<u64>()).unwrap();
    let last = owner
        .trace(owner.snapshot().unwrap().trace_len - 1)
        .unwrap()
        .unwrap();
    assert_eq!((last.id, last.generation), (0, 2)); // reuse only AFTER actual free/debit
    drop(ticket);
    drain(&mut owner);
    let before = core.snapshot().unwrap();
    assert_eq!(drain(&mut owner).freed, 0); // exact-once debit
    assert_eq!(core.snapshot().unwrap(), before);
    set_generation_fault(&core, u64::MAX).unwrap();
    assert!(matches!(
        try_prepare(&op, CoreClass::Ledger, Layout::new::<u64>()),
        Err(CoreFailure::ArithmeticOverflow)
    ));
    assert_eq!(core.snapshot().unwrap(), before);
    finish(
        core,
        owner,
        "16-record-fixture/rotating-scan/generation/exact-once",
    );
}

fn error_child_and_custom_lifetimes() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let op = CoreOp {
        core: &core,
        deadline: deadline(),
        stop: &stop,
    };
    let weak = core.try_downgrade(op.deadline, &stop).unwrap();
    let alias = core.try_share(op.deadline, &stop).unwrap();
    let upgraded = weak.try_upgrade(op.deadline, &stop).unwrap().unwrap();
    let source = try_string(
        &op,
        CoreClass::Handshake,
        "locked rustls General owned source",
    )
    .unwrap();
    let mut error = rustls::Error::General(source);
    let before = core.snapshot().unwrap();
    let replacement = rustls::Error::General(
        try_string(&op, CoreClass::Handshake, "replacement backing").unwrap(),
    );
    let extracted = match std::mem::replace(&mut error, replacement) {
        rustls::Error::General(text) => text,
        _ => unreachable!("fixture constructs only General"),
    };
    assert_eq!(extracted, "locked rustls General owned source");
    assert!(core.snapshot().unwrap().counts.0[1] > before.counts.0[1]);
    drop(error);
    drain(&mut owner);
    assert_eq!(core.snapshot().unwrap().counts, before.counts);
    drop((core, alias, upgraded));
    assert!(weak.try_upgrade(deadline(), &stop).unwrap().is_none());
    let pending = owner.snapshot().unwrap();
    assert_eq!(pending.strong, 0);
    assert_eq!(pending.weak, 1);
    assert_eq!(pending.states[2], 1);
    assert!(matches!(
        weak.try_upgrade(Instant::now(), &stop),
        Err(CoreFailure::DeadlineExpired)
    ));
    assert!(!drain(&mut owner).complete); // child outlives user core and error wrapper
    assert_eq!(owner.snapshot().unwrap().counts, pending.counts);
    drop(extracted); // family retirement still reaches original H ledger
    stop.store(true, Ordering::Release);
    assert!(matches!(
        weak.try_upgrade(Instant::now(), &stop),
        Err(CoreFailure::Stopped)
    ));
    assert_eq!(drain(&mut owner).freed, 1); // mandatory cleanup ignores stop/expiry
    assert!(!drain(&mut owner).complete); // Weak holds charged core/control
    assert_eq!(
        weak.snapshot().unwrap().counts,
        bootstrap_layouts().unwrap().1
    );
    drop(weak);
    let report = drain(&mut owner);
    assert!(report.complete);
    assert_eq!(aggregate_snapshot().unwrap(), Counts::default());
    println!(
        "E1 real-General/extraction/replacement/aliases/Weak/original-H/late-cleanup: {pending:?} final={report:?}"
    );
}

fn leak_pending_then_recover() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let op = CoreOp {
        core: &core,
        deadline: deadline(),
        stop: &stop,
    };
    let mut bytes = try_vec_bytes(&op, CoreClass::Handshake, 123).unwrap();
    bytes.extend_from_slice(b"deliberately leaked until fixture recovery");
    let (p, len, cap) = (bytes.as_mut_ptr(), bytes.len(), bytes.capacity());
    std::mem::forget(bytes);
    drop(core);
    let before = owner.snapshot().unwrap();
    assert_eq!((before.strong, before.weak), (0, 0)); // E1-R01: refs0 cannot bypass LIVE record
    assert!(!drain(&mut owner).complete);
    assert_eq!(owner.snapshot().unwrap().counts, before.counts);
    assert_eq!(before.states[2], 1);
    // Recovery is a fixture-only use of saved lawful Global raw ownership;
    // no leaked charge was forgiven while bytes were unreachable.
    drop(unsafe { Vec::from_raw_parts(p, len, cap) });
    assert!(drain(&mut owner).complete);
    assert_eq!(aggregate_snapshot().unwrap(), Counts::default());
    println!("E1 leak/pending/recovered-physical-free: {before:?}");
}

fn concurrent_protocol() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let end = deadline();
    let success = AtomicUsize::new(0);
    let busy = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let aliases = AtomicUsize::new(0);
    // Scoped std threads and their harness allocations are explicitly untagged;
    // they are not application proof or ignored production baseline objects.
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                set_null_fault(0);
                let op = CoreOp {
                    core: &core,
                    deadline: end,
                    stop: &stop,
                };
                for _ in 0..128 {
                    if let Ok(alias) = core.try_share(end, &stop) {
                        if let Ok(weak) = alias.try_downgrade(end, &stop) {
                            if let Ok(Some(upgraded)) = weak.try_upgrade(end, &stop) {
                                drop(upgraded);
                                aliases.fetch_add(1, Ordering::Relaxed);
                            }
                            drop(weak);
                        }
                        drop(alias);
                    }
                    match try_vec_bytes(&op, CoreClass::Handshake, 64) {
                        Ok(bytes) => {
                            drop(bytes);
                            success.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(CoreFailure::AdmissionBusy) => {
                            busy.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(other) => panic!("unexpected author fixture result {other:?}"),
                    }
                    std::thread::yield_now(); // harness only, never allocator/core loop
                }
                done.fetch_add(1, Ordering::Release);
            });
        }
        // Fixed driver work, one try-lock per scan, no admission spin/retry loop.
        for _ in 0..4096 {
            let _ = collect_retired(&mut owner, RECORDS);
            if done.load(Ordering::Acquire) == 8 {
                break;
            }
            std::thread::yield_now();
        }
    });
    drain(&mut owner);
    let s = core.snapshot().unwrap();
    assert!(success.load(Ordering::Relaxed) > 0);
    assert!(aliases.load(Ordering::Relaxed) > 0);
    assert_eq!(s.strong, 1);
    assert_eq!(s.weak, 0);
    assert_eq!(s.prepare_calls, success.load(Ordering::Relaxed));
    assert_eq!(s.consumed, s.prepare_calls);
    assert_eq!(s.frees, s.prepare_calls);
    assert_eq!(s.debits, s.frees);
    assert!(s.peak <= T);
    assert_eq!(hook_counts(), (0, 0, 0));
    println!(
        "E1 concurrent success={} refused-busy={} aliases={} done={}",
        success.load(Ordering::Relaxed),
        busy.load(Ordering::Relaxed),
        aliases.load(Ordering::Relaxed),
        done.load(Ordering::Relaxed)
    );
    finish(
        core,
        owner,
        "concurrent-admission-refs-upgrade-retire-free-generation",
    );
}

fn last_strong_upgrade_and_collection() {
    // Upgrade linearizes at strong CAS(old,old+1); Drop linearizes at fetch_sub.
    // If CAS wins, terminal zero cannot occur until the returned Strong drops.
    // If terminal Drop wins, load0 returns None or a stale nonzero CAS fails
    // once (Busy); no path writes 0->1. The borrowed live Weak keeps core alive
    // through either operation. Collection requires refs0 AND every record FREE.
    // All barriers are harness-only, outside refcount/allocator/retire operations.
    for schedule in ["upgrade-wins", "drop-wins", "overlapping-start"] {
        let stop = AtomicBool::new(false);
        let (core, mut owner) = bootstrap(&stop);
        let end = deadline();
        let weak = core.try_downgrade(end, &stop).unwrap();
        let ticket = try_prepare(
            &CoreOp {
                core: &core,
                deadline: end,
                stop: &stop,
            },
            CoreClass::Handshake,
            Layout::new::<u64>(),
        )
        .unwrap();
        let start = std::sync::Barrier::new(3);
        let ordered = std::sync::Barrier::new(2);
        let (result, concurrent_collect) = std::thread::scope(|scope| {
            let start_ref = &start;
            let ordered_ref = &ordered;
            let dropper = scope.spawn(move || {
                start_ref.wait();
                if schedule == "upgrade-wins" {
                    ordered_ref.wait();
                }
                drop(core); // the ORIGINAL LAST Strong, no observer Strong retained
                if schedule == "drop-wins" {
                    ordered_ref.wait();
                }
            });
            let upgrader = scope.spawn(|| {
                set_null_fault(0);
                start.wait();
                if schedule == "drop-wins" {
                    ordered.wait();
                }
                let result = weak.try_upgrade(end, &stop);
                if schedule == "upgrade-wins" {
                    ordered.wait();
                }
                assert_eq!(direct_counts(), (0, 0));
                assert_eq!(system_counts(), (0, 0));
                result // only the actual returned Strong can remain live
            });
            start.wait();
            let report = collect_retired(&mut owner, RECORDS);
            dropper.join().unwrap();
            (upgrader.join().unwrap(), report)
        });
        assert!(!concurrent_collect.complete);
        assert_eq!(
            (concurrent_collect.freed, concurrent_collect.debited),
            (0, 0)
        );
        let outcome = match &result {
            Ok(Some(_)) => "Some(Strong)",
            Ok(None) => "None/terminal-zero",
            Err(CoreFailure::AdmissionBusy) if schedule == "overlapping-start" => "Busy/stale-CAS",
            _ => panic!("unexpected last-reference outcome"),
        };
        if schedule == "upgrade-wins" {
            assert!(matches!(&result, Ok(Some(_))));
        }
        if schedule == "drop-wins" {
            assert!(matches!(&result, Ok(None)));
        }
        let returned = result.unwrap_or(None); // Busy is recorded, never retried
        let held = owner.snapshot().unwrap();
        assert_eq!(held.strong, usize::from(returned.is_some()));
        assert_eq!(held.weak, 1);
        assert_eq!(held.states, [RECORDS - 1, 1, 0, 0, 0]);
        assert_eq!(held.aggregate, held.counts);
        drop(ticket);
        let retired = owner.snapshot().unwrap();
        assert_eq!(retired.states, [RECORDS - 1, 0, 0, 1, 0]);
        assert_eq!(retired.counts, held.counts);
        assert_eq!((retired.frees, retired.debits), (0, 0));
        let child_free = drain(&mut owner);
        assert_eq!((child_free.freed, child_free.debited), (1, 1));
        assert!(!child_free.complete);
        let (layouts, base) = bootstrap_layouts().unwrap();
        let handles_only = owner.snapshot().unwrap();
        assert_eq!(handles_only.states, [RECORDS, 0, 0, 0, 0]);
        assert_eq!(handles_only.counts, base);
        assert!(!drain(&mut owner).complete); // ALL records FREE; live Weak still retains core
        let traces: [Trace; 5] = std::array::from_fn(|i| owner.trace(i).unwrap().unwrap());
        assert_eq!(traces.map(|t| t.kind), [1, 2, 4, 5, 6]);
        assert_eq!(traces[3].total, held.counts.0[0]);
        assert_eq!(traces[4].total, base.0[0]);
        if returned.is_none() {
            // Separate post-terminal observation, NOT a retry of a Busy upgrade.
            assert!(weak.try_upgrade(end, &stop).unwrap().is_none());
            assert_eq!(owner.snapshot().unwrap().strong, 0);
        }
        drop(weak);
        if let Some(strong) = &returned {
            let only_returned = strong.snapshot().unwrap();
            assert_eq!((only_returned.strong, only_returned.weak), (1, 0));
            assert_eq!(only_returned.states, [RECORDS, 0, 0, 0, 0]);
            assert!(!drain(&mut owner).complete); // returned Strong alone retains core
            assert_eq!(strong.snapshot().unwrap().counts, base);
        }
        drop(returned);
        let refs_zero = owner.snapshot().unwrap();
        assert_eq!((refs_zero.strong, refs_zero.weak), (0, 0));
        assert_eq!(refs_zero.states, [RECORDS, 0, 0, 0, 0]);
        assert_eq!(refs_zero.counts, base); // all core evidence copied before final free
        set_null_fault(0); // external scalar free witnesses, no core-owned post-free evidence
        let report = drain(&mut owner);
        assert!(report.complete);
        assert_eq!(
            (report.freed, report.debited, report.remaining),
            (0, 0, Some(0))
        );
        assert_eq!(system_counts(), (0, 3)); // exactly THREE bootstrap frees
        let witnesses = free_witnesses();
        let frees = [
            witnesses[0].unwrap(),
            witnesses[1].unwrap(),
            witnesses[2].unwrap(),
        ];
        assert_eq!(witnesses[3], None);
        for (i, expected) in [layouts[1], layouts[2], layouts[0]].into_iter().enumerate() {
            assert_eq!(frees[i].bytes, expected.size());
            assert_eq!(frees[i].before, base);
            assert_eq!(frees[i].returned, base); // aggregate nonzero through core free
        }
        for i in 0..frees.len() {
            for j in 0..i {
                assert_ne!(frees[i].base, frees[j].base);
            }
        }
        assert_eq!(aggregate_snapshot().unwrap(), Counts::default());
        let repeated = drain(&mut owner); // owner=None; no core access after final free
        assert!(repeated.complete);
        assert_eq!((repeated.freed, repeated.debited), (0, 0));
        assert_eq!(system_counts(), (0, 3));
        assert_eq!(free_witnesses(), witnesses);
        assert_eq!(aggregate_snapshot().unwrap(), Counts::default());
        println!(
            "E1-R01 {schedule}: outcome={outcome}; overlap={concurrent_collect:?}; refs0={refs_zero:?}; traces={traces:?}; external-returned-frees={frees:?}; final={report:?}; repeated={repeated:?}"
        );
    }
}

fn late_original_context_after_preparation() {
    for mode in [1, 2] {
        for pod_leaf in [false, true] {
            let stop = AtomicBool::new(false);
            let (core, mut owner) = bootstrap(&stop);
            let base = bootstrap_layouts().unwrap().1;
            let reuse_end = deadline(); // pre-existing independent reuse context, no refresh
            let original_end = if mode == 1 {
                reuse_end
            } else {
                Instant::now() + Duration::from_millis(100)
            };
            let op = CoreOp {
                core: &core,
                deadline: original_end,
                stop: &stop,
            };
            let expected = if mode == 1 {
                CoreFailure::Stopped
            } else {
                CoreFailure::DeadlineExpired
            };
            set_null_fault(0);
            set_late_context_fault(mode);
            if pod_leaf {
                assert_eq!(
                    try_prepaid_pod_box(&op, CoreClass::Handshake, Pod::new(7)),
                    Err(expected)
                );
            } else {
                assert_eq!(try_vec_bytes(&op, CoreClass::Handshake, 64), Err(expected));
            }
            let prepared = late_context_witness().unwrap();
            assert!(!prepared.stopped && prepared.deadline_live);
            assert_eq!(prepared.successes, 1); // actual NONNULL System success before fault
            assert_eq!(prepared.snapshot.states, [RECORDS - 1, 1, 0, 0, 0]);
            assert_eq!(prepared.snapshot.aggregate, prepared.snapshot.counts);
            assert_eq!(
                (prepared.snapshot.consumed, prepared.snapshot.downstream),
                (0, 0)
            );
            assert_eq!(op.deadline, original_end);
            if mode == 1 {
                assert!(stop.load(Ordering::Acquire));
            } else {
                assert!(!stop.load(Ordering::Acquire));
                assert!(Instant::now() >= original_end);
            }
            let retired = core.snapshot().unwrap();
            assert_eq!(retired.states, [RECORDS - 1, 0, 0, 1, 0]);
            assert_eq!(retired.counts, prepared.snapshot.counts);
            assert_eq!(retired.aggregate, retired.counts);
            assert_eq!(
                (
                    retired.consumed,
                    retired.downstream,
                    retired.retired,
                    retired.frees,
                    retired.debits
                ),
                (0, 0, 1, 0, 0)
            );
            assert_eq!(direct_counts(), (1, 0));
            assert_eq!(system_counts(), (1, 0));
            assert_eq!(system_successes(), 1);
            set_late_context_fault(0); // only scheduling control reset; original stop/expiry retained
            assert_eq!(drain(&mut owner).freed, 1); // no new deadline, still stopped/expired
            assert_eq!(system_counts(), (1, 1));
            let clean = core.snapshot().unwrap();
            assert_eq!(clean.counts, base);
            assert_eq!((clean.frees, clean.debits), (1, 1));
            let traces: [Trace; 5] = std::array::from_fn(|i| owner.trace(i).unwrap().unwrap());
            assert_eq!(traces.map(|t| t.kind), [1, 2, 4, 5, 6]);
            assert_eq!(traces[3].total, retired.counts.0[0]); // 5 AFTER free but BEFORE debit
            assert_eq!(traces[4].total, base.0[0]);
            assert!(
                traces
                    .iter()
                    .all(|t| (t.id, t.generation, t.class) == (0, 1, CoreClass::Handshake))
            );
            let freed = free_witnesses()[0].unwrap();
            assert_eq!(
                (freed.before, freed.returned),
                (retired.counts, retired.counts)
            );
            assert_eq!(drain(&mut owner).freed, 0);
            assert_eq!(core.snapshot().unwrap(), clean);
            // Generation reuse is a separate eligible context created BEFORE
            // expiry. It is never a fresh cleanup deadline or retry of refusal.
            stop.store(false, Ordering::Release);
            let reused = try_prepare(
                &CoreOp {
                    core: &core,
                    deadline: reuse_end,
                    stop: &stop,
                },
                CoreClass::Handshake,
                Layout::new::<u64>(),
            )
            .unwrap();
            let reused_trace = owner
                .trace(owner.snapshot().unwrap().trace_len - 1)
                .unwrap()
                .unwrap();
            assert_eq!((reused_trace.id, reused_trace.generation), (0, 2));
            drop(reused);
            stop.store(true, Ordering::Release);
            assert_eq!(drain(&mut owner).freed, 1);
            assert_eq!(core.snapshot().unwrap().counts, base);
            println!(
                "E1-R03 mode={mode} pod={pod_leaf} returned={expected:?}; original-deadline-expired={}; prepared={prepared:?}; retired={retired:?}; trace={traces:?}; returned-free={freed:?}; reuse={reused_trace:?}; controlled preparation scheduling, System latency NOT_PROVEN",
                Instant::now() >= original_end
            );
            finish(
                core,
                owner,
                "E1-R03 original-stop/real-expiry/prepaid-retirement",
            );
            assert_eq!(aggregate_snapshot().unwrap(), Counts::default());
        }
    }
}

fn concurrent_quota_races() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let base = bootstrap_layouts().unwrap().1.0[0];
    for (class, cap) in [
        (CoreClass::Configuration, (T - base) / 2 + 4096),
        (CoreClass::Handshake, H / 2 + 4096),
    ] {
        let barrier = std::sync::Barrier::new(3); // untagged harness coordination
        let accepted = AtomicUsize::new(0);
        let total_quota = AtomicUsize::new(0);
        let handshake_quota = AtomicUsize::new(0);
        let admission_busy = AtomicUsize::new(0);
        let end = deadline();
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    set_null_fault(0);
                    let op = CoreOp {
                        core: &core,
                        deadline: end,
                        stop: &stop,
                    };
                    barrier.wait(); // simultaneous start, no admission retries
                    let result = try_vec_bytes(&op, class, cap);
                    match &result {
                        Ok(_) => {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(reason) => {
                            assert_eq!(direct_counts(), (0, 0));
                            assert_eq!(system_counts(), (0, 0));
                            assert_eq!(system_successes(), 0);
                            match reason {
                                CoreFailure::TotalQuota => &total_quota,
                                CoreFailure::HandshakeQuota => &handshake_quota,
                                CoreFailure::AdmissionBusy => &admission_busy,
                                other => panic!("unexpected near-limit author result {other:?}"),
                            }
                            .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    barrier.wait(); // keep successful backing LIVE through both admissions
                    barrier.wait();
                    drop(result);
                });
            }
            barrier.wait();
            barrier.wait();
            let s = core.snapshot().unwrap();
            assert_eq!(accepted.load(Ordering::Relaxed), 1);
            let outcomes = [
                total_quota.load(Ordering::Relaxed),
                handshake_quota.load(Ordering::Relaxed),
                admission_busy.load(Ordering::Relaxed),
            ];
            assert_eq!(outcomes.iter().sum::<usize>(), 1);
            assert_eq!(outcomes[usize::from(class == CoreClass::Configuration)], 0);
            assert!(s.counts.0[0] <= T && s.counts.0[1] <= H);
            assert_eq!(s.aggregate, s.counts);
            println!(
                "E1-R02 simultaneous {class:?}: accepted=1 [TotalQuota,HandshakeQuota,AdmissionBusy]={outcomes:?} {s:?}"
            );
            barrier.wait();
        });
        drain(&mut owner);
    }
    finish(
        core,
        owner,
        "concurrent-T-H-no-overcommit/rejected-actual-calls-zero",
    );
}

fn deterministic_quota_and_busy() {
    let stop = AtomicBool::new(false);
    let (core, mut owner) = bootstrap(&stop);
    let base = bootstrap_layouts().unwrap().1;
    let end = deadline(); // same pre-existing context for A, B and Busy
    for (class, cap, expected) in [
        (
            CoreClass::Configuration,
            (T - base.0[0]) / 2 + 4096,
            CoreFailure::TotalQuota,
        ),
        (
            CoreClass::Handshake,
            H / 2 + 4096,
            CoreFailure::HandshakeQuota,
        ),
    ] {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                set_null_fault(0);
                let op = CoreOp {
                    core: &core,
                    deadline: end,
                    stop: &stop,
                };
                let bytes = try_vec_bytes(&op, class, cap).unwrap();
                assert_eq!(direct_counts(), (1, 0));
                assert_eq!(system_counts(), (1, 0));
                assert_eq!(system_successes(), 1);
                barrier.wait(); // actual success, LIVE, admission guard released
                barrier.wait(); // keep A's returned backing through B/Busy assertions
                drop(bytes);
            });
            barrier.wait();
            let before = core.snapshot().unwrap();
            assert_eq!(before.states, [RECORDS - 1, 0, 1, 0, 0]);
            assert_eq!(before.aggregate, before.counts);
            set_null_fault(0);
            let op = CoreOp {
                core: &core,
                deadline: end,
                stop: &stop,
            };
            assert_eq!(try_vec_bytes(&op, class, cap), Err(expected));
            assert_eq!(direct_counts(), (0, 0));
            assert_eq!(system_counts(), (0, 0));
            assert_eq!(system_successes(), 0);
            assert_eq!(core.snapshot().unwrap(), before); // includes downstream/consumed/records
            // The very same request now loses the intentionally held admission
            // lock BEFORE quota inspection. No retries hide the Busy outcome.
            let held = hold_admission().unwrap();
            assert_eq!(
                try_vec_bytes(&op, class, cap),
                Err(CoreFailure::AdmissionBusy)
            );
            assert_eq!(direct_counts(), (0, 0));
            assert_eq!(system_counts(), (0, 0));
            assert_eq!(system_successes(), 0);
            drop(held);
            assert_eq!(core.snapshot().unwrap(), before);
            println!(
                "E1-R02 two-phase {class:?}: exact={expected:?}; separate AdmissionBusy; B direct/System/success/downstream delta=0; unchanged={before:?}"
            );
            barrier.wait();
        });
        let retired = core.snapshot().unwrap();
        assert_eq!(retired.states, [RECORDS - 1, 0, 0, 1, 0]);
        assert_eq!(retired.frees + 1, retired.retired);
        assert_eq!(retired.frees, retired.debits);
        let report = drain(&mut owner);
        assert_eq!((report.freed, report.debited), (1, 1));
        assert_eq!(core.snapshot().unwrap().counts, base);
        let clean = core.snapshot().unwrap();
        assert_eq!(drain(&mut owner).freed, 0);
        assert_eq!(core.snapshot().unwrap(), clean);
    }
    finish(
        core,
        owner,
        "E1-R02 deterministic-quota/separate-Busy/exact-once",
    );
}

fn custody_loss_is_pending_last() {
    let stop = AtomicBool::new(false);
    let (core, owner) = bootstrap(&stop);
    let op = CoreOp {
        core: &core,
        deadline: deadline(),
        stop: &stop,
    };
    let weak = core.try_downgrade(op.deadline, &stop).unwrap();
    let text = try_string(
        &op,
        CoreClass::Handshake,
        "orphan remains charged until actual free",
    )
    .unwrap();
    let before = owner.snapshot().unwrap();
    drop(owner); // no free, no self-cycle, no fabricated completion
    drop(core);
    assert!(weak.snapshot().unwrap().orphaned);
    assert_eq!(weak.snapshot().unwrap().counts, before.counts);
    drop(text); // bounded hook works after custodian/user loss, backing stays retired
    let after = weak.snapshot().unwrap();
    assert_eq!(after.counts, before.counts);
    assert_eq!(after.states[3], 1);
    assert_eq!(after.frees, 0);
    assert_eq!(after.debits, 0);
    assert!(weak.try_upgrade(deadline(), &stop).unwrap().is_none());
    drop(weak);
    assert_eq!(aggregate_snapshot().unwrap(), after.counts);
    println!("E1 custody loss intentionally retains orphan/raw backing, NOT completion: {after:?}");
}

#[test]
fn alloc45_u4_core_e1_author_matrix() {
    // Sequential fixture; custody-loss case intentionally orphaned LAST. No
    // other process/application/TLS allocation is asserted covered by this test.
    bootstrap_and_layouts();
    refusal_and_nulls();
    exact_limits();
    finite_leaves_and_traces();
    moving_resize();
    records_and_generation();
    error_child_and_custom_lifetimes();
    leak_pending_then_recover();
    concurrent_protocol();
    last_strong_upgrade_and_collection();
    concurrent_quota_races();
    deterministic_quota_and_busy();
    late_original_context_after_preparation();
    custody_loss_is_pending_last();
    assert_eq!(hook_counts(), (0, 0, 0));
}
