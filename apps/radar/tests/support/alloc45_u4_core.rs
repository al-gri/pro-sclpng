//! ALLOC45-U4-CORE-E1 only. No production allocator or arbitrary callback API.
//! Pinned contracts: ADR0004 section 12 / Rust 48a229cea (GlobalAlloc, Layout,
//! Vec::from_raw_parts, Box::new and const/no-Drop native TLS).
//! All Global-produced pointers use this family. Direct System bootstrap
//! pointers remain internal; foreign pointers must never enter this family.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, UnsafeCell};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

pub const T: usize = 8_388_608;
pub const H: usize = 1_048_576;
pub const BUFFER: usize = 65_536;
pub const DIAGNOSTIC: usize = 131_072;
pub const RECORDS: usize = 16; // fixture capacity, never a production input cap
const FREE: usize = 0;
const PREPAID: usize = 1;
const LIVE: usize = 2;
const RETIRED: usize = 3;
const FREEING: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreClass {
    Configuration,
    Handshake,
    RxPlain,
    TxPlain,
    TxRecord,
    HandshakeTxRecord,
    Diagnostic,
    Ledger,
}

// Fixed scalar evidence; no formatting/allocating error construction. These
// map to ADR's ResourceRefusal reasons, DeadlineExpired, Stopped and closure stop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreFailure {
    TotalQuota,
    HandshakeQuota,
    BufferQuota,
    ArithmeticOverflow,
    AllocatorNull,
    ReferenceCountOverflow,
    AdmissionBusy,
    DeadlineExpired,
    Stopped,
    UnresolvedClosure,
}

#[repr(C)]
struct Header {
    core: *mut LedgerCore,
    index: usize,
    generation: u64,
    class: CoreClass,
}

pub fn family_layout(payload: Layout) -> Result<(Layout, usize), CoreFailure> {
    let (layout, offset) = Layout::new::<Header>()
        .extend(payload)
        .map_err(|_| CoreFailure::ArithmeticOverflow)?;
    Ok((layout.pad_to_align(), offset))
}

fn context(deadline: Instant, stop: &AtomicBool) -> Result<(), CoreFailure> {
    if stop.load(Ordering::Acquire) {
        Err(CoreFailure::Stopped)
    } else if Instant::now() >= deadline {
        Err(CoreFailure::DeadlineExpired)
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Counts(pub [usize; 6]); // T,H,RxPlain,TxPlain,TxRecord,Diagnostic

impl Counts {
    fn add(self, delta: Self) -> Result<Self, CoreFailure> {
        let mut result = self;
        for (value, add) in result.0.iter_mut().zip(delta.0) {
            *value = value
                .checked_add(add)
                .ok_or(CoreFailure::ArithmeticOverflow)?;
        }
        for (i, limit) in [T, H, BUFFER, BUFFER, BUFFER, DIAGNOSTIC]
            .into_iter()
            .enumerate()
        {
            if result.0[i] > limit {
                return Err(match i {
                    0 => CoreFailure::TotalQuota,
                    1 => CoreFailure::HandshakeQuota,
                    _ => CoreFailure::BufferQuota,
                });
            }
        }
        Ok(result)
    }

    fn remove(self, delta: Self) -> Result<Self, CoreFailure> {
        let mut result = self;
        for (value, remove) in result.0.iter_mut().zip(delta.0) {
            *value = value
                .checked_sub(remove)
                .ok_or(CoreFailure::ArithmeticOverflow)?;
        }
        Ok(result)
    }
}

fn charge(class: CoreClass, payload: Layout, physical: Layout) -> Counts {
    let n = physical.size();
    let mut result = Counts([n, 0, 0, 0, 0, 0]);
    match class {
        CoreClass::Handshake => result.0[1] = n,
        CoreClass::RxPlain => result.0[2] = n,
        CoreClass::TxPlain => result.0[3] = n,
        CoreClass::TxRecord => result.0[4] = n,
        CoreClass::HandshakeTxRecord => {
            result.0[1] = n;
            result.0[4] = n;
        }
        CoreClass::Diagnostic => result.0[5] = payload.size(), // overhead also in T
        CoreClass::Configuration | CoreClass::Ledger => {}
    }
    result
}

// One nonallocating try-lock serializes all ledgers' aggregate counters, record
// fields, generations and physical debits. BSS is disclosed separately; it is
// not heap backing or an exempt heap bootstrap. Each core also pays its own
// inline counters/records/refcounts/control and evidence backings in T.
struct Aggregate(UnsafeCell<Counts>);
// SAFETY: access exclusively under ADMISSION, including bootstrap and cleanup.
unsafe impl Sync for Aggregate {}
static AGGREGATE: Aggregate = Aggregate(UnsafeCell::new(Counts([0; 6])));
static ADMISSION: AtomicBool = AtomicBool::new(false);

struct Admission;
impl Admission {
    fn acquire() -> Result<Self, CoreFailure> {
        ADMISSION
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map(|_| Self)
            .map_err(|_| CoreFailure::AdmissionBusy)
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        ADMISSION.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
struct Block {
    base: NonNull<u8>,
    physical: Layout,
    offset: usize,
}

#[derive(Clone, Copy)]
struct Entry {
    block: Block,
    payload: Layout,
    class: CoreClass,
    generation: u64,
    charge: Counts,
}
struct Record {
    state: AtomicUsize,
    generation: AtomicU64,
    entry: UnsafeCell<Option<Entry>>,
}
impl Record {
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(FREE),
            generation: AtomicU64::new(0),
            entry: UnsafeCell::new(None),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Trace {
    pub kind: u8, // 1 reserve,2 alloc,3 null/rollback,4 retire observed,5 free returned,6 debit
    pub id: usize,
    pub generation: u64,
    pub total: usize,
    pub bytes: usize,
    pub class: CoreClass,
}
struct Book {
    counts: Counts,
    peak: usize,
    trace_len: usize,
}
struct LedgerCore {
    strong: AtomicUsize,
    weak: AtomicUsize,
    orphaned: AtomicBool,
    records: [Record; RECORDS],
    book: UnsafeCell<Book>,
    core_block: Block,
    diagnostic: Block,
    custodian: Block,
    prepare_calls: AtomicUsize,
    nulls: AtomicUsize,
    consumed: AtomicUsize,
    downstream: AtomicUsize,
    retired: AtomicUsize,
    frees: AtomicUsize,
    debits: AtomicUsize,
}
// SAFETY: immutable backing fields, atomic reference/state fields; Book and
// Entry accessed only under ADMISSION. Core stays alive until custodian drain
// observes no external refs or records. Orphaned cores are never reclaimed.
unsafe impl Sync for LedgerCore {}

#[repr(C)]
struct ReclaimControl {
    cursor: usize,
}

thread_local! {
    // Pinned native const/no-Drop TLS. No lazy allocation, queue, logging,
    // thread::current(), destructor or mutex in allocator hooks.
    static PLAN: Cell<*mut CallPlan> = const { Cell::new(ptr::null_mut()) };
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
    static FAULT_AT: Cell<usize> = const { Cell::new(0) };
    static DIRECT_CALLS: Cell<usize> = const { Cell::new(0) };
    static ACTUAL_CALLS: Cell<usize> = const { Cell::new(0) };
    static ACTUAL_SUCCESSES: Cell<usize> = const { Cell::new(0) };
    static DIRECT_FREES: Cell<usize> = const { Cell::new(0) };
    static FREE_WITNESSES: Cell<[Option<FreeWitness>; 4]> = const { Cell::new([None; 4]) };
    static LATE_CONTEXT_FAULT: Cell<u8> = const { Cell::new(0) };
    static LATE_CONTEXT_WITNESS: Cell<Option<PreparationWitness>> = const { Cell::new(None) };
}
static RECURSIONS: AtomicUsize = AtomicUsize::new(0);
static PLAN_VIOLATIONS: AtomicUsize = AtomicUsize::new(0);
static TAGGED_REALLOC_STOPS: AtomicUsize = AtomicUsize::new(0);

pub fn set_null_fault(nth: usize) {
    FAULT_AT.set(nth);
    DIRECT_CALLS.set(0);
    ACTUAL_CALLS.set(0);
    ACTUAL_SUCCESSES.set(0);
    DIRECT_FREES.set(0);
    FREE_WITNESSES.set([None; 4]);
}
pub fn direct_counts() -> (usize, usize) {
    (DIRECT_CALLS.get(), DIRECT_FREES.get())
}
pub fn system_counts() -> (usize, usize) {
    (ACTUAL_CALLS.get(), DIRECT_FREES.get())
}
pub fn system_successes() -> usize {
    ACTUAL_SUCCESSES.get()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FreeWitness {
    pub base: usize,
    pub bytes: usize,
    pub before: Counts,
    pub returned: Counts,
}
pub fn free_witnesses() -> [Option<FreeWitness>; 4] {
    FREE_WITNESSES.get()
}
// Modes: 0 disabled, 1 original stop, 2 real original Instant expiry. Private
// direct-preparation scheduling only; never read by allocator/refcount/retire.
pub fn set_late_context_fault(mode: u8) {
    LATE_CONTEXT_FAULT.set(mode);
    LATE_CONTEXT_WITNESS.set(None);
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreparationWitness {
    pub snapshot: Snapshot,
    pub stopped: bool,
    pub deadline_live: bool,
    pub successes: usize,
}
pub fn late_context_witness() -> Option<PreparationWitness> {
    LATE_CONTEXT_WITNESS.get()
}
pub fn hook_counts() -> (usize, usize, usize) {
    (
        RECURSIONS.load(Ordering::Relaxed),
        PLAN_VIOLATIONS.load(Ordering::Relaxed),
        TAGGED_REALLOC_STOPS.load(Ordering::Relaxed),
    )
}

// Fault injection applies only to explicit protected preparation, never harness
// allocations or the already prepaid infallible Box allocation.
unsafe fn system_prepare(layout: Layout) -> *mut u8 {
    let n = DIRECT_CALLS.get() + 1;
    DIRECT_CALLS.set(n);
    if FAULT_AT.get() == n {
        ptr::null_mut()
    } else {
        ACTUAL_CALLS.set(ACTUAL_CALLS.get() + 1);
        // SAFETY: caller checked valid nonzero physical Layout and admitted it.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            ACTUAL_SUCCESSES.set(ACTUAL_SUCCESSES.get() + 1);
        }
        pointer
    }
}
unsafe fn system_free(block: Block) {
    // All direct frees hold ADMISSION. Copy only scalars outside the freed
    // backing; no pointer dereference or core access after returned free.
    let before = unsafe { *AGGREGATE.0.get() };
    // SAFETY: original System base/physical Layout, one custodial/rollback free.
    unsafe { System.dealloc(block.base.as_ptr(), block.physical) };
    let n = DIRECT_FREES.get();
    let mut witnesses = FREE_WITNESSES.get();
    if n < witnesses.len() {
        witnesses[n] = Some(FreeWitness {
            base: block.base.as_ptr() as usize,
            bytes: block.physical.size(),
            before,
            returned: unsafe { *AGGREGATE.0.get() },
        });
        FREE_WITNESSES.set(witnesses);
    }
    DIRECT_FREES.set(n + 1);
}

unsafe fn allocate_block(payload: Layout) -> Result<Block, CoreFailure> {
    let (physical, offset) = family_layout(payload)?;
    // SAFETY: bootstrap/preparation callers have aggregate admission.
    let base =
        NonNull::new(unsafe { system_prepare(physical) }).ok_or(CoreFailure::AllocatorNull)?;
    Ok(Block {
        base,
        physical,
        offset,
    })
}
unsafe fn write_header(
    block: Block,
    core: *mut LedgerCore,
    index: usize,
    generation: u64,
    class: CoreClass,
) {
    // SAFETY: fresh physical backing includes aligned Header before payload.
    unsafe {
        block.base.as_ptr().cast::<Header>().write(Header {
            core,
            index,
            generation,
            class,
        })
    };
}

pub struct CoreStrong {
    core: NonNull<LedgerCore>,
}
pub struct CoreWeak {
    core: NonNull<LedgerCore>,
}
pub struct CoreReclaimOwner {
    core: Option<NonNull<LedgerCore>>,
}
// SAFETY: handles only access synchronized core fields and hold lifetime refs.
unsafe impl Send for CoreStrong {}
unsafe impl Sync for CoreStrong {}
unsafe impl Send for CoreWeak {}
unsafe impl Sync for CoreWeak {}

pub fn bootstrap_layouts() -> Result<([Layout; 3], Counts), CoreFailure> {
    let payloads = [
        Layout::new::<LedgerCore>(),
        Layout::array::<u8>(DIAGNOSTIC).map_err(|_| CoreFailure::ArithmeticOverflow)?,
        Layout::new::<ReclaimControl>(),
    ];
    let mut counts = Counts::default();
    let mut physical = [Layout::new::<Header>(); 3];
    for (i, payload) in payloads.into_iter().enumerate() {
        physical[i] = family_layout(payload)?.0;
        counts = counts.add(charge(
            if i == 1 {
                CoreClass::Diagnostic
            } else {
                CoreClass::Ledger
            },
            payload,
            physical[i],
        ))?;
    }
    Ok((physical, counts))
}

pub fn fixture_sizes() -> [usize; 12] {
    // Header, core, record, entry, book, custodian, trace; remaining handle/plan
    // values are inline/stack, with no extra heap allocation or exempt backing.
    [
        std::mem::size_of::<Header>(),
        std::mem::size_of::<LedgerCore>(),
        std::mem::size_of::<Record>(),
        std::mem::size_of::<Entry>(),
        std::mem::size_of::<Book>(),
        std::mem::size_of::<ReclaimControl>(),
        std::mem::size_of::<Trace>(),
        std::mem::size_of::<CoreStrong>(),
        std::mem::size_of::<CoreWeak>(),
        std::mem::size_of::<CoreReclaimOwner>(),
        std::mem::size_of::<Prepaid>(),
        std::mem::size_of::<CallPlan>(),
    ]
}

pub fn try_bootstrap(
    deadline: Instant,
    stop: &AtomicBool,
) -> Result<(CoreStrong, CoreReclaimOwner), CoreFailure> {
    context(deadline, stop)?;
    let (_, counts) = bootstrap_layouts()?;
    let _guard = Admission::acquire()?;
    context(deadline, stop)?;
    // SAFETY: guarded aggregate, reservation imported into core exactly once.
    let aggregate = unsafe { &mut *AGGREGATE.0.get() };
    *aggregate = aggregate.add(counts)?;
    let payloads = [
        Layout::new::<LedgerCore>(),
        Layout::array::<u8>(DIAGNOSTIC).map_err(|_| CoreFailure::ArithmeticOverflow)?,
        Layout::new::<ReclaimControl>(),
    ];
    let mut blocks = [None; 3];
    for (i, layout) in payloads.into_iter().enumerate() {
        // SAFETY: all three physical layouts were admitted atomically above.
        match unsafe { allocate_block(layout) } {
            Ok(block) => blocks[i] = Some(block),
            Err(error) => {
                for block in blocks.into_iter().flatten() {
                    // SAFETY: internal raw backings have no aliases, free before rollback.
                    unsafe { system_free(block) };
                }
                *aggregate = aggregate.remove(counts)?;
                return Err(error);
            }
        }
    }
    // All Options were set above. Pattern avoids an allocating panic path.
    let [Some(core_block), Some(diagnostic), Some(custodian)] = blocks else {
        return Err(CoreFailure::UnresolvedClosure);
    };
    if let Err(error) = context(deadline, stop) {
        for block in [core_block, diagnostic, custodian] {
            unsafe { system_free(block) };
        }
        *aggregate = aggregate.remove(counts)?;
        return Err(error);
    }
    // SAFETY: distinct admitted blocks, properly aligned, no std ownership.
    let core = unsafe {
        NonNull::new_unchecked(
            core_block
                .base
                .as_ptr()
                .add(core_block.offset)
                .cast::<LedgerCore>(),
        )
    };
    unsafe {
        for (index, block) in [core_block, diagnostic, custodian].into_iter().enumerate() {
            write_header(
                block,
                ptr::null_mut(),
                0,
                0,
                if index == 1 {
                    CoreClass::Diagnostic
                } else {
                    CoreClass::Ledger
                },
            );
        }
        diagnostic
            .base
            .as_ptr()
            .add(diagnostic.offset)
            .write_bytes(0, DIAGNOSTIC);
        custodian
            .base
            .as_ptr()
            .add(custodian.offset)
            .cast::<ReclaimControl>()
            .write(ReclaimControl { cursor: 0 });
        core.as_ptr().write(LedgerCore {
            strong: AtomicUsize::new(1),
            weak: AtomicUsize::new(0),
            orphaned: AtomicBool::new(false),
            records: std::array::from_fn(|_| Record::new()),
            book: UnsafeCell::new(Book {
                counts,
                peak: counts.0[0],
                trace_len: 0,
            }),
            core_block,
            diagnostic,
            custodian,
            prepare_calls: AtomicUsize::new(0),
            nulls: AtomicUsize::new(0),
            consumed: AtomicUsize::new(0),
            downstream: AtomicUsize::new(0),
            retired: AtomicUsize::new(0),
            frees: AtomicUsize::new(0),
            debits: AtomicUsize::new(0),
        });
    }
    Ok((CoreStrong { core }, CoreReclaimOwner { core: Some(core) }))
}

fn increment(counter: &AtomicUsize) -> Result<(), CoreFailure> {
    let old = counter.load(Ordering::Acquire);
    let new = old
        .checked_add(1)
        .ok_or(CoreFailure::ReferenceCountOverflow)?;
    counter
        .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        .map(|_| ())
        .map_err(|_| CoreFailure::AdmissionBusy)
}
impl CoreStrong {
    fn get(&self) -> &LedgerCore {
        // SAFETY: this strong ref prevents final custodial reclamation.
        unsafe { self.core.as_ref() }
    }
    pub fn try_share(&self, deadline: Instant, stop: &AtomicBool) -> Result<Self, CoreFailure> {
        context(deadline, stop)?;
        increment(&self.get().strong)?;
        Ok(Self { core: self.core })
    }
    pub fn try_downgrade(
        &self,
        deadline: Instant,
        stop: &AtomicBool,
    ) -> Result<CoreWeak, CoreFailure> {
        context(deadline, stop)?;
        increment(&self.get().weak)?;
        Ok(CoreWeak { core: self.core })
    }
    pub fn snapshot(&self) -> Result<Snapshot, CoreFailure> {
        snapshot(self.core)
    }
}
impl Drop for CoreStrong {
    fn drop(&mut self) {
        self.get().strong.fetch_sub(1, Ordering::AcqRel);
    }
}
impl CoreWeak {
    fn get(&self) -> &LedgerCore {
        // SAFETY: this weak ref prevents final custodial reclamation.
        unsafe { self.core.as_ref() }
    }
    pub fn try_upgrade(
        &self,
        deadline: Instant,
        stop: &AtomicBool,
    ) -> Result<Option<CoreStrong>, CoreFailure> {
        context(deadline, stop)?;
        let old = self.get().strong.load(Ordering::Acquire);
        if old == 0 {
            return Ok(None);
        }
        let new = old
            .checked_add(1)
            .ok_or(CoreFailure::ReferenceCountOverflow)?;
        self.get()
            .strong
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| CoreFailure::AdmissionBusy)?;
        Ok(Some(CoreStrong { core: self.core }))
    }
    pub fn snapshot(&self) -> Result<Snapshot, CoreFailure> {
        snapshot(self.core)
    }
}
impl Drop for CoreWeak {
    fn drop(&mut self) {
        self.get().weak.fetch_sub(1, Ordering::AcqRel);
    }
}
impl Drop for CoreReclaimOwner {
    fn drop(&mut self) {
        if let Some(core) = self.core {
            // SAFETY: custodian owns ledger lifetime; premature loss deliberately
            // leaves all raw backings and charges intact, no completion/free.
            unsafe { core.as_ref() }
                .orphaned
                .store(true, Ordering::Release);
        }
    }
}

pub struct CoreOp<'a> {
    pub core: &'a CoreStrong,
    pub deadline: Instant,
    pub stop: &'a AtomicBool,
}
pub struct Prepaid {
    core: NonNull<LedgerCore>,
    index: usize,
    entry: Entry,
    consumed: bool,
}
impl Drop for Prepaid {
    fn drop(&mut self) {
        if !self.consumed {
            // SAFETY: record holds independent core backing custody until free.
            unsafe {
                retire(
                    self.core.as_ptr(),
                    self.index,
                    self.entry.generation,
                    PREPAID,
                )
            };
        }
    }
}

// Called with ADMISSION held. Trace storage is inside paid Diagnostic payload.
fn trace(
    core: &LedgerCore,
    book: &mut Book,
    kind: u8,
    id: usize,
    generation: u64,
    bytes: usize,
    class: CoreClass,
) {
    let capacity = DIAGNOSTIC / std::mem::size_of::<Trace>();
    if book.trace_len < capacity {
        let value = Trace {
            kind,
            id,
            generation,
            total: book.counts.0[0],
            bytes,
            class,
        };
        // SAFETY: Diagnostic aligned at least to Header (>= Trace), initialized
        // Copy records, bounded index; same lock as trace readers/writers.
        unsafe {
            core.diagnostic
                .base
                .as_ptr()
                .add(core.diagnostic.offset)
                .cast::<Trace>()
                .add(book.trace_len)
                .write(value)
        };
        book.trace_len += 1;
    }
}

pub fn try_prepare(
    op: &CoreOp<'_>,
    class: CoreClass,
    layout: Layout,
) -> Result<Prepaid, CoreFailure> {
    context(op.deadline, op.stop)?;
    if layout.size() == 0 {
        return Err(CoreFailure::UnresolvedClosure);
    } // helpers use lawful dangling convention
    let (physical, _) = family_layout(layout)?;
    let delta = charge(class, layout, physical);
    let _guard = Admission::acquire()?;
    context(op.deadline, op.stop)?;
    let core = op.core.get();
    // SAFETY: mutation of Book/Entry/global counters under ADMISSION.
    let book = unsafe { &mut *core.book.get() };
    let aggregate = unsafe { &mut *AGGREGATE.0.get() };
    let next = aggregate.add(delta)?;
    let local = book.counts.add(delta)?;
    let index = core
        .records
        .iter()
        .position(|r| r.state.load(Ordering::Acquire) == FREE)
        .ok_or(CoreFailure::AdmissionBusy)?;
    let record = &core.records[index];
    let generation = record
        .generation
        .load(Ordering::Relaxed)
        .checked_add(1)
        .ok_or(CoreFailure::ArithmeticOverflow)?;
    *aggregate = next;
    book.counts = local;
    book.peak = book.peak.max(local.0[0]);
    trace(core, book, 1, index, generation, physical.size(), class);
    core.prepare_calls.fetch_add(1, Ordering::Relaxed);
    // SAFETY: exact full physical Layout admitted, lock covers rollback too.
    let block = match unsafe { allocate_block(layout) } {
        Ok(block) => block,
        Err(error) => {
            let local = book.counts.remove(delta)?;
            let global = aggregate.remove(delta)?;
            book.counts = local;
            *aggregate = global;
            core.nulls.fetch_add(1, Ordering::Relaxed);
            trace(core, book, 3, index, generation, physical.size(), class);
            return Err(error);
        }
    };
    let entry = Entry {
        block,
        payload: layout,
        class,
        generation,
        charge: delta,
    };
    unsafe {
        write_header(block, op.core.core.as_ptr(), index, generation, class);
        *record.entry.get() = Some(entry);
    }
    record.generation.store(generation, Ordering::Relaxed);
    record.state.store(PREPAID, Ordering::Release);
    trace(core, book, 2, index, generation, physical.size(), class);
    let ticket = Prepaid {
        core: op.core.core,
        index,
        entry,
        consumed: false,
    };
    let fault = LATE_CONTEXT_FAULT.get();
    if fault != 0 {
        // Controlled scheduling AFTER actual success/PREPAID publication and
        // BEFORE the unchanged context check. No injected clock/error, callback
        // or wait in GlobalAlloc, reference operations or retirement. This
        // deliberate delay supplies NO bound on System allocation/free latency.
        LATE_CONTEXT_WITNESS.set(Some(PreparationWitness {
            snapshot: snapshot_locked(core),
            stopped: op.stop.load(Ordering::Acquire),
            deadline_live: Instant::now() < op.deadline,
            successes: ACTUAL_SUCCESSES.get(),
        }));
        if fault == 1 {
            op.stop.store(true, Ordering::Release);
        } else if fault == 2 {
            std::thread::sleep(op.deadline.saturating_duration_since(Instant::now()));
        }
    }
    context(op.deadline, op.stop)?; // original context; late preparation retires, stays charged
    Ok(ticket)
}

struct CallPlan {
    ticket: *mut Prepaid,
}
struct PublishedPlan;
impl PublishedPlan {
    fn enter(plan: &mut CallPlan) -> Result<Self, CoreFailure> {
        if !PLAN.get().is_null() {
            return Err(CoreFailure::UnresolvedClosure);
        }
        PLAN.set(plan);
        Ok(Self)
    }
}
impl Drop for PublishedPlan {
    fn drop(&mut self) {
        PLAN.set(ptr::null_mut());
    }
}

unsafe fn retire(core: *mut LedgerCore, index: usize, generation: u64, from: usize) {
    // SAFETY: Header/ticket immutable identity and independent record lifetime.
    let ledger = unsafe { &*core };
    let record = &ledger.records[index];
    if record.generation.load(Ordering::Relaxed) == generation {
        // Finish instrumentation BEFORE release publication; collector may free
        // backing/core immediately after publication. No later ledger access.
        ledger.retired.fetch_add(1, Ordering::Relaxed);
        let _ = record
            .state
            .compare_exchange(from, RETIRED, Ordering::Release, Ordering::Relaxed);
    }
}

pub struct FamilyAllocator;
// SAFETY: every allocation from this Global has Header+payload at public Layout
// offset; dealloc uses original payload Layout to recover exact System base/F.
// Tagged blocks retire only. No foreign pointers are accepted by these methods.
unsafe impl GlobalAlloc for FamilyAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if IN_HOOK.replace(true) {
            RECURSIONS.fetch_add(1, Ordering::Relaxed);
        }
        let plan = PLAN.get();
        let result = if !plan.is_null() {
            // SAFETY: synchronous borrowed stack plan published before the single
            // source-verified allocation, destroyed after it, never shared.
            let ticket = unsafe { &mut *(*plan).ticket };
            if !ticket.consumed && ticket.entry.payload == layout {
                ticket.consumed = true;
                let core = unsafe { ticket.core.as_ref() };
                core.consumed.fetch_add(1, Ordering::Relaxed);
                core.records[ticket.index]
                    .state
                    .store(LIVE, Ordering::Release);
                unsafe {
                    ticket
                        .entry
                        .block
                        .base
                        .as_ptr()
                        .add(ticket.entry.block.offset)
                }
            } else {
                // Internal plan breach, NOT a quota/refusal/returned Box error.
                // No such call is authorized: finite Pod/raw leaf checked before
                // invocation. Unsupported callers cannot use PublishedPlan.
                // No emergency uncharged fallback. Any observation is E1 FAIL.
                PLAN_VIOLATIONS.fetch_add(1, Ordering::Relaxed);
                ptr::null_mut()
            }
        } else if let Ok((physical, offset)) = family_layout(layout) {
            // Untagged test harness only, no protected-application claim.
            let base = unsafe { System.alloc(physical) };
            if !base.is_null() {
                unsafe {
                    base.cast::<Header>().write(Header {
                        core: ptr::null_mut(),
                        index: 0,
                        generation: 0,
                        class: CoreClass::Configuration,
                    })
                };
                unsafe { base.add(offset) }
            } else {
                base
            }
        } else {
            ptr::null_mut()
        };
        IN_HOOK.set(false);
        result
    }

    unsafe fn dealloc(&self, payload: *mut u8, layout: Layout) {
        if IN_HOOK.replace(true) {
            RECURSIONS.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok((physical, offset)) = family_layout(layout) {
            // SAFETY: only same-family Global-produced payload/original Layout.
            let base = unsafe { payload.sub(offset) };
            let header = unsafe { &*base.cast::<Header>() };
            if header.core.is_null() {
                unsafe { System.dealloc(base, physical) };
            } else {
                // Copy header before publication, never touch backing afterward.
                let (core, index, generation) = (header.core, header.index, header.generation);
                unsafe { retire(core, index, generation, LIVE) };
            }
        }
        IN_HOOK.set(false);
    }

    unsafe fn realloc(&self, payload: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Tagged ordinary infallible std resizing is outside the finite plan.
        // try_resize_bytes below explicitly admits moving old+new instead.
        let Ok((_, offset)) = family_layout(layout) else {
            return ptr::null_mut();
        };
        let header = unsafe { &*payload.sub(offset).cast::<Header>() };
        if !header.core.is_null() {
            TAGGED_REALLOC_STOPS.fetch_add(1, Ordering::Relaxed);
            return ptr::null_mut(); // NOT advertised as lawful infallible resize
        }
        let Ok(new_layout) = Layout::from_size_align(new_size, layout.align()) else {
            return ptr::null_mut();
        };
        let new = unsafe { self.alloc(new_layout) };
        if !new.is_null() {
            unsafe {
                ptr::copy_nonoverlapping(payload, new, layout.size().min(new_size));
                self.dealloc(payload, layout);
            }
        }
        new
    }
}

fn take_raw(ticket: &mut Prepaid) -> Result<NonNull<u8>, CoreFailure> {
    let mut plan = CallPlan { ticket };
    let _published = PublishedPlan::enter(&mut plan)?;
    // Selected Global allocation, NOT adopting bare System pointer. This exact
    // fallible raw leaf makes one public alloc(L) call and does not optimize its
    // failure into an infallible handle_alloc_error path.
    let result = unsafe { std::alloc::alloc(ticket.entry.payload) };
    NonNull::new(result).ok_or(CoreFailure::UnresolvedClosure)
}
pub fn try_vec_bytes(
    op: &CoreOp<'_>,
    class: CoreClass,
    capacity: usize,
) -> Result<Vec<u8>, CoreFailure> {
    context(op.deadline, op.stop)?;
    if !PLAN.get().is_null() {
        return Err(CoreFailure::UnresolvedClosure);
    }
    if capacity == 0 {
        return Ok(Vec::new());
    }
    let layout = Layout::array::<u8>(capacity).map_err(|_| CoreFailure::ArithmeticOverflow)?;
    let mut ticket = try_prepare(op, class, layout)?;
    context(op.deadline, op.stop)?;
    let pointer = take_raw(&mut ticket)?;
    // SAFETY: Global-produced byte pointer, original alignment/capacity, len0.
    Ok(unsafe { Vec::from_raw_parts(pointer.as_ptr(), 0, capacity) })
}
pub fn try_string(op: &CoreOp<'_>, class: CoreClass, text: &str) -> Result<String, CoreFailure> {
    let mut bytes = try_vec_bytes(op, class, text.len())?;
    bytes.extend_from_slice(text.as_bytes()); // exact prepaid capacity, no growth
    // SAFETY: copied str bytes, UTF-8; ownership move adds no backing.
    Ok(unsafe { String::from_utf8_unchecked(bytes) })
}

#[repr(C, align(128))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pod {
    value: [u64; 4],
} // sealed named leaf, no heap/nested Drop
impl Pod {
    pub fn new(value: u64) -> Self {
        Self { value: [value; 4] }
    }
}

pub fn try_prepaid_pod_box(
    op: &CoreOp<'_>,
    class: CoreClass,
    value: Pod,
) -> Result<Box<Pod>, CoreFailure> {
    context(op.deadline, op.stop)?;
    if !PLAN.get().is_null() {
        return Err(CoreFailure::UnresolvedClosure);
    }
    let mut ticket = try_prepare(op, class, Layout::new::<Pod>())?;
    context(op.deadline, op.stop)?;
    let mut plan = CallPlan {
        ticket: &mut ticket,
    };
    // Missing/nested plan stops BEFORE Box invocation. Sealed Pod's pinned
    // Box::new path has exactly Layout::new::<Pod>(), no constructor/drop calls.
    let _published = PublishedPlan::enter(&mut plan)?;
    op.core.get().downstream.fetch_add(1, Ordering::Relaxed);
    Ok(Box::new(value)) // nonnull prepared storage, never fault injection here
}
pub fn try_resize_bytes(
    op: &CoreOp<'_>,
    class: CoreClass,
    bytes: &mut Vec<u8>,
    capacity: usize,
) -> Result<(), CoreFailure> {
    context(op.deadline, op.stop)?;
    if capacity < bytes.len() {
        return Err(CoreFailure::UnresolvedClosure);
    }
    // Reject foreign/untagged ownership and category changes BEFORE allocating.
    if bytes.capacity() != 0 {
        let layout =
            Layout::array::<u8>(bytes.capacity()).map_err(|_| CoreFailure::ArithmeticOverflow)?;
        let (_, offset) = family_layout(layout)?;
        // bytes may only come from this process's Global family. No foreign
        // raw transfer is supported; checking a foreign pointer is forbidden.
        let header = unsafe { &*bytes.as_ptr().sub(offset).cast::<Header>() };
        if header.core != op.core.core.as_ptr() || header.class != class {
            return Err(CoreFailure::UnresolvedClosure);
        }
    }
    let mut next = try_vec_bytes(op, class, capacity)?; // old+new paid, also shrink
    next.extend_from_slice(bytes);
    let old = std::mem::replace(bytes, next);
    drop(old); // retire only, old charge persists until returned physical free
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Snapshot {
    pub counts: Counts,
    pub aggregate: Counts,
    pub peak: usize,
    pub strong: usize,
    pub weak: usize,
    pub states: [usize; 5],
    pub prepare_calls: usize,
    pub nulls: usize,
    pub consumed: usize,
    pub downstream: usize,
    pub retired: usize,
    pub frees: usize,
    pub debits: usize,
    pub orphaned: bool,
    pub trace_len: usize,
}
fn snapshot_locked(core: &LedgerCore) -> Snapshot {
    // SAFETY: caller holds ADMISSION; core lifetime protected by handle/custody.
    let book = unsafe { &*core.book.get() };
    let mut states = [0; 5];
    for record in &core.records {
        states[record.state.load(Ordering::Acquire)] += 1;
    }
    Snapshot {
        counts: book.counts,
        aggregate: unsafe { *AGGREGATE.0.get() },
        peak: book.peak,
        strong: core.strong.load(Ordering::Acquire),
        weak: core.weak.load(Ordering::Acquire),
        states,
        prepare_calls: core.prepare_calls.load(Ordering::Relaxed),
        nulls: core.nulls.load(Ordering::Relaxed),
        consumed: core.consumed.load(Ordering::Relaxed),
        downstream: core.downstream.load(Ordering::Relaxed),
        retired: core.retired.load(Ordering::Relaxed),
        frees: core.frees.load(Ordering::Relaxed),
        debits: core.debits.load(Ordering::Relaxed),
        orphaned: core.orphaned.load(Ordering::Acquire),
        trace_len: book.trace_len,
    }
}
fn snapshot(core: NonNull<LedgerCore>) -> Result<Snapshot, CoreFailure> {
    let _guard = Admission::acquire()?;
    Ok(snapshot_locked(unsafe { core.as_ref() }))
}
pub fn aggregate_snapshot() -> Result<Counts, CoreFailure> {
    let _guard = Admission::acquire()?;
    Ok(unsafe { *AGGREGATE.0.get() })
}
impl CoreReclaimOwner {
    pub fn snapshot(&self) -> Result<Snapshot, CoreFailure> {
        self.core
            .ok_or(CoreFailure::UnresolvedClosure)
            .and_then(snapshot)
    }
    pub fn trace(&self, index: usize) -> Result<Option<Trace>, CoreFailure> {
        let _guard = Admission::acquire()?;
        let core = self.core.ok_or(CoreFailure::UnresolvedClosure)?;
        let core = unsafe { core.as_ref() };
        let book = unsafe { &*core.book.get() };
        if index >= book.trace_len {
            return Ok(None);
        }
        Ok(Some(unsafe {
            core.diagnostic
                .base
                .as_ptr()
                .add(core.diagnostic.offset)
                .cast::<Trace>()
                .add(index)
                .read()
        }))
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReclaimReport {
    pub scanned: usize,
    pub freed: usize,
    pub debited: usize,
    pub remaining: Option<usize>, // None on contended scan; never fabricated zero
    pub complete: bool,
    pub busy: bool,
    pub arithmetic_blocked: bool,
}
pub fn collect_retired(owner: &mut CoreReclaimOwner, max_steps: usize) -> ReclaimReport {
    let Some(pointer) = owner.core else {
        return ReclaimReport {
            complete: true,
            ..ReclaimReport::default()
        };
    };
    let Ok(_guard) = Admission::acquire() else {
        return ReclaimReport {
            busy: true,
            ..ReclaimReport::default()
        };
    };
    // SAFETY: owner independently holds all bootstrap backings/core alive.
    let core = unsafe { pointer.as_ref() };
    let book = unsafe { &mut *core.book.get() };
    let mut report = ReclaimReport::default();
    // Rotating bounded scan ensures max_steps<16 does not starve later records.
    let control = unsafe {
        &mut *core
            .custodian
            .base
            .as_ptr()
            .add(core.custodian.offset)
            .cast::<ReclaimControl>()
    };
    let start = control.cursor;
    for step in 0..max_steps.min(RECORDS) {
        let index = (start + step) % RECORDS;
        let record = &core.records[index];
        report.scanned += 1;
        if record
            .state
            .compare_exchange(RETIRED, FREEING, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // SAFETY: immutable initialized Entry published before retirement;
            // no producer/header accesses it after release retirement. No reuse
            // until free returns, debit and FREE release under admission lock.
            let Some(entry) = (unsafe { *record.entry.get() }) else {
                report.arithmetic_blocked = true;
                continue;
            };
            // Checked transaction computed under the lock, applied ONLY after
            // returned free. An inconsistent debit keeps backing/charge pending.
            let (Ok(local), Ok(global)) = (
                book.counts.remove(entry.charge),
                unsafe { *AGGREGATE.0.get() }.remove(entry.charge),
            ) else {
                record.state.store(RETIRED, Ordering::Release);
                report.arithmetic_blocked = true;
                continue;
            };
            trace(
                core,
                book,
                4,
                index,
                entry.generation,
                entry.block.physical.size(),
                entry.class,
            );
            unsafe { system_free(entry.block) };
            core.frees.fetch_add(1, Ordering::Relaxed);
            report.freed += 1;
            trace(
                core,
                book,
                5,
                index,
                entry.generation,
                entry.block.physical.size(),
                entry.class,
            ); // still charged
            book.counts = local;
            unsafe { *AGGREGATE.0.get() = global };
            core.debits.fetch_add(1, Ordering::Relaxed);
            report.debited += 1;
            trace(
                core,
                book,
                6,
                index,
                entry.generation,
                entry.block.physical.size(),
                entry.class,
            );
            unsafe { *record.entry.get() = None };
            record.state.store(FREE, Ordering::Release);
        }
    }
    control.cursor = (start + report.scanned) % RECORDS;
    report.remaining = Some(book.counts.0[0]);
    if core.strong.load(Ordering::Acquire) == 0
        && core.weak.load(Ordering::Acquire) == 0
        && core
            .records
            .iter()
            .all(|r| r.state.load(Ordering::Acquire) == FREE)
        && !core.orphaned.load(Ordering::Acquire)
    {
        // Capture every last witness BEFORE core free, ledger last. Internal
        // blocks never acquire external/self ledger refs or std ownership.
        let blocks = [core.diagnostic, core.custodian, core.core_block];
        let total = blocks
            .iter()
            .try_fold(0usize, |n, b| n.checked_add(b.physical.size()));
        let Some(total) = total else {
            report.arithmetic_blocked = true;
            return report;
        };
        let delta = Counts([total, 0, 0, 0, 0, DIAGNOSTIC]);
        let Ok(global) = unsafe { *AGGREGATE.0.get() }.remove(delta) else {
            report.arithmetic_blocked = true;
            return report;
        };
        if book.counts != delta {
            report.arithmetic_blocked = true;
            return report;
        }
        owner.core = None;
        for block in blocks {
            unsafe { system_free(block) };
        }
        // Conservative aggregate debit after ALL bootstrap frees return. No
        // ledger access after the last System free; only stack scalar witness.
        unsafe { *AGGREGATE.0.get() = global };
        report.remaining = Some(0);
        report.complete = true;
    }
    report
}

// Narrow scalar fault controls for author tests; no arbitrary callback/T API.
pub fn hold_admission() -> Result<impl Drop, CoreFailure> {
    Admission::acquire()
}
/// # Safety
/// Fixture driver must have exclusive refcount-operation/cleanup custody and
/// restore the true count before any handle Drop or custodial reclamation.
/// Artificial counts are not ownership references. Not a production operation.
pub unsafe fn set_refcount_fault(core: &CoreStrong, strong: bool, value: usize) {
    if strong {
        core.get().strong.store(value, Ordering::Relaxed);
    } else {
        core.get().weak.store(value, Ordering::Relaxed);
    }
}
pub fn set_generation_fault(core: &CoreStrong, value: u64) -> Result<(), CoreFailure> {
    let _guard = Admission::acquire()?;
    // Only fixture FREE records, under admission, no live header can observe it.
    if core
        .get()
        .records
        .iter()
        .any(|r| r.state.load(Ordering::Acquire) != FREE)
    {
        return Err(CoreFailure::AdmissionBusy);
    }
    for record in &core.get().records {
        record.generation.store(value, Ordering::Relaxed);
    }
    Ok(())
}
pub fn unused_pod_ticket(op: &CoreOp<'_>, class: CoreClass) -> Result<(), CoreFailure> {
    // Explicit elision simulation, NOT evidence that compiler elided Box::new.
    let _ticket = try_prepare(op, class, Layout::new::<Pod>())?;
    Ok(())
}
pub fn nested_plan_stop(op: &CoreOp<'_>) -> Result<(), CoreFailure> {
    let mut ticket = try_prepare(op, CoreClass::Ledger, Layout::new::<Pod>())?;
    let mut plan = CallPlan {
        ticket: &mut ticket,
    };
    let _published = PublishedPlan::enter(&mut plan)?;
    let _unentered = try_prepaid_pod_box(op, CoreClass::Ledger, Pod::new(1))?;
    Ok(())
}
