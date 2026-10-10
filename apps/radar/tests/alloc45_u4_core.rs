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
    set_refcount_fault(&core, true, usize::MAX);
    assert!(matches!(
        core.try_share(op.deadline, &stop),
        Err(CoreFailure::ReferenceCountOverflow)
    ));
    set_refcount_fault(&core, true, 1);
    set_refcount_fault(&core, false, usize::MAX);
    assert!(matches!(
        core.try_downgrade(op.deadline, &stop),
        Err(CoreFailure::ReferenceCountOverflow)
    ));
    set_refcount_fault(&core, false, 0);
    let weak = core.try_downgrade(op.deadline, &stop).unwrap();
    set_refcount_fault(&core, true, usize::MAX);
    assert!(matches!(
        weak.try_upgrade(op.deadline, &stop),
        Err(CoreFailure::ReferenceCountOverflow)
    ));
    set_refcount_fault(&core, true, 1);
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
        let refused = AtomicUsize::new(0);
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
                    let result = try_vec_bytes(&op, class, cap);
                    match &result {
                        Ok(_) => {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(
                            CoreFailure::TotalQuota
                            | CoreFailure::HandshakeQuota
                            | CoreFailure::AdmissionBusy,
                        ) => {
                            assert_eq!(direct_counts(), (0, 0));
                            assert_eq!(system_counts(), (0, 0));
                            refused.fetch_add(1, Ordering::Relaxed);
                        }
                        other => panic!("unexpected near-limit author result {other:?}"),
                    }
                    barrier.wait(); // keep successful backing LIVE through both admissions
                    barrier.wait();
                    drop(result);
                });
            }
            barrier.wait();
            let s = core.snapshot().unwrap();
            assert_eq!(accepted.load(Ordering::Relaxed), 1);
            assert_eq!(refused.load(Ordering::Relaxed), 1);
            assert!(s.counts.0[0] <= T && s.counts.0[1] <= H);
            assert_eq!(s.aggregate, s.counts);
            println!("E1 concurrent near-limit {class:?}: accepted=1 refused=1 {s:?}");
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
    concurrent_quota_races();
    custody_loss_is_pending_last();
    assert_eq!(hook_counts(), (0, 0, 0));
}
