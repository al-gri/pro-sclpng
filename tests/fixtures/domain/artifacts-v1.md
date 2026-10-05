# Artifact descriptor golden bytes — AF fixtures

Status: **PROPOSED**, SPEC-001 proposal revision2. origin=synthetic; descriptor_schema=1; body_schema=1; policy_version=1 where relevant. No real Bitget data, network capture, secrets or executable decoder. [Descriptor contract](../../../specs/domain/artifacts-v1.md); [named negative vectors](../../../specs/domain/review-vectors-v2.md).

Each hex block represents EXACT bytes after removing ASCII line breaks and decoding hex. The Markdown wrapper/hex digits/newlines are NOT bytes to hash. Descriptor refs hash decoded descriptor bytes; body hashes hash decoded BODY bytes. This separation avoids self-referential descriptor hashes. Exact EOF and the lengths below are assertions for a future codec/resolver test, not a round-trip-generated oracle.

Calculated offline in this worker session using Python3.13.5 hashlib and `openssl dgst -sha256 -binary` (OpenSSL3.5.5); all five descriptor and five body digests matched, native exit0. This is cross-tool byte calculation outside any future Rust codec, not a claim of cryptographically independent implementations (the tools may share a library). No repository Rust model/test was run. Parsing, loader correctness and real source applicability remain unproved.

| ID | Kind / identity / revision | Descriptor bytes | Body bytes | Expected descriptor ArtifactRef |
|---|---|---:|---:|---|
| AF-N1 | Normalizer / normalizer /1 | 86 | 23 | sha256:4fb4d8059243f24a6f7fbd33b8773bce4ea520c19f01b2074404b2c6ab5e7ded |
| AF-B1 | Basis / synthetic-basis /1 | 91 | 27 | sha256:ede784f697e400d548203d6322bb68c0029c22246f2839624bc3838d09803e80 |
| AF-C1 | Config / config /1 | 154 | 135 | sha256:c3e9f77e412956d8e5e6c95e734140150e464d17b7c5f251dbaca0658390cef0 |
| AF-F1 | FeedProfile / stream/1 /1 | 228 | 172 | sha256:eb68b12f25f22563a84362680147f58b0b8c123d533672e1e9f3a5ddff9cfbbd |
| AF-V1 | Verification / stream/1/raw/10 /1 | 307 | 245 | sha256:f96bff366da5fc98965b81a3bd55c24a99969ce6e426d272ecd878b8ad9605de |

All descriptors: magicPSAD,format1,provenance_kindSynthetic2,provenance_text='fixture:synthetic' (17 ASCII bytes). Dependencies are sorted exact descriptor refs. This closure of five artifacts is an isolated descriptor fixture, NOT a complete archive/instrument registry/normalization implementation.

## AF-N1

Body is exactly ASCII `synthetic-normalizer-1` followed by LF. This is a test artifact label, NOT executable normalization logic. Expected body SHA256: `031e12d6c823b0690c8f20f6354bb6f6e7b5852116759efdbe57bf1c72643a8d`. No dependencies.

Descriptor:
```text
505341440100010a6e6f726d616c697a65720100000001001700000000000000031e12d6c823b0690c8f20f6354bb6f6
 e7b5852116759efdbe57bf1c72643a8d021100666978747572653a73796e7468657469630000
```
Body:
```text
73796e7468657469632d6e6f726d616c697a65722d310a
```

## AF-B1

Body ASCII `synthetic-profile-checks-1` plus LF. Expected body SHA256: `7b7b0c0cfb108323df644a6d17d4227824769756b579f5bc16eba2440186bcfa`. No dependencies. A synthetic resolver may explicitly attest the scope facts named in a logical vector; this label alone attests none in production.

Descriptor:
```text
505341440100080f73796e7468657469632d62617369730100000001001b000000000000007b7b0c0cfb108323df644a
6d17d4227824769756b579f5bc16eba2440186bcfa021100666978747572653a73796e7468657469630000
```
Body:
```text
73796e7468657469632d70726f66696c652d636865636b732d310a
```

## AF-C1

Expected config: proposal_revision2,config1,norm1 pointing AF-N1,provenanceSynthetic,UnknownOnSilence1,deadlineSome100,warmup_countSome1,warmup_elapsedSome0,allow_quiet=false,two_sided=true,gateDurable3,pending_frames2,pending_raw_bytes256,pending_outputs4,pending_wait50,quiet_max_lifetimeNone. Dependency AF-N1 only. Expected body SHA256: `d3827df81929780ebd89539b091c14a2873f9934743947c080d01a7211544546`.

Descriptor:
```text
5053414401000206636f6e6669670100000001008700000000000000d3827df81929780ebd89539b091c14a2873f9934
743947c080d01a7211544546021100666978747572653a73796e7468657469630100477368613235363a346662346438
303539323433663234613666376662643333623837373362636534656135323063313966303162323037343430346232
63366162356537646564
```
Body:
```text
02000100000001000000477368613235363a346662346438303539323433663234613666376662643333623837373362
636534656135323063313966303162323037343430346232633661623565376465640201016400000000000000010100
000001000000000000000000010302000000000100000000000004000000320000000000000000
```

## AF-F1

Expected stream1,InstrumentRef(SYN,Spot1,spot,ABCUSD),BookNormal channel1,profile1,supported normalizers exactly[AF-N1],profile_basisAF-B1. Dependencies sorted[AF-N1,AF-B1]. Expected body SHA256: `e353efcf0522137235c3e202acee70a4f1ad8b79767bbd568757aef3e2cd2f06`.

Descriptor:
```text
505341440100030873747265616d2f31010000000100ac00000000000000e353efcf0522137235c3e202acee70a4f1ad
8b79767bbd568757aef3e2cd2f06021100666978747572653a73796e7468657469630200477368613235363a34666234
643830353932343366323461366637666264333362383737336263653465613532306331396630316232303734343034
623263366162356537646564477368613235363a65646537383466363937653430306435343832303364363332326262
363863303032396332323234366632383339363234626333383338643039383033653830
```
Body:
```text
010000000353594e010473706f740641424355534401010000000100477368613235363a346662346438303539323433
663234613666376662643333623837373362636534656135323063313966303162323037343430346232633661623565
37646564477368613235363a656465373834663639376534303064353438323033643633323262623638633030323963
32323234366632383339363234626333383338643039383033653830
```

## AF-V1

Expected Scope: Archive16x01,Session16x02,Clock1,Stream1,slot1,tag(spec1,connection_epoch1,subscription_epoch1,book_epochSome1),config1,norm1,profile1,barrier9. Scope encoding is93 bytes. Then raw_record10,Snapshot1,raw_sample10,not_before9,valid_untilNone,output_count1,commitment below,continuity_basisAF-B1,BasisRecords[9,10]. Dependencies sorted[AF-C1,AF-F1,AF-B1]. Expected body SHA256: `13b3cb6cbd76450a7199804c90487d7b91aa4ec386b55f3e8327a7a54738bde0`.

Descriptor:
```text
505341440100050f73747265616d2f312f7261772f3130010000000100f50000000000000013b3cb6cbd76450a719980
4c90487d7b91aa4ec386b55f3e8327a7a54738bde0021100666978747572653a73796e74686574696303004773686132
35363a633365396637376534313239353664386535653663393565373334313430313530653436346431376237633566
32353164626163613036353833393063656630477368613235363a656236386231326632356632323536336138343336
323638303134376635386230623863313233643533333637326531653966336135646466663963666262644773686132
35363a656465373834663639376534303064353438323033643633323262623638633030323963323232343666323833
39363234626333383338643039383033653830
```
Body:
```text
010101010101010101010101010101010202020202020202020202020202020201000000010000000100000001000000
0100000000000000010000000000000001010000000000000001000000010000000100000009000000000000000a0000
0000000000010a00000000000000090000000000000000010000003a5c48fcddc983224bbfd8907540b2beee16fb527f
980d8e8e9539ed473323ea477368613235363a6564653738346636393765343030643534383230336436333232626236
3863303032396332323234366632383339363234626333383338643039383033653830020009000000000000000a0000
0000000000
```

The committed output sequence is49 bytes: PSCO,schema1,count1,Snapshot tag1,two entries(Bid price2000 qty1,Ask price2002 qty1):
```text
5053434f010001000000010200000001d007000000000000010000000000000002d2070000000000000100000000000000
```
Expected SHA256: `3a5c48fcddc983224bbfd8907540b2beee16fb527f980d8e8e9539ed473323ea`.
A synthetic proof record11 referring to raw10 may produce SourceCandidateKey((A,10),0,1),EventId(A,11,0,1),cursor/available_at(11,0),as_of.record_frontier11 when all initial scope guards hold. Raw admission10 alone produces no confirmed book effect. Duplicate proof at12 produces ALREADY_APPLIED and no new EventId; original availability stays(11,0).

## Negative expectations / limits

Mutating bytes but retaining a claimed ref: ArtifactDigestMismatch; changing body length: ArtifactLengthMismatch; missing dependency: MissingArtifact; rebinding same logical key: ArtifactIdentityConflict; supported parsing without real byte verification: ArtifactUnverified. These behaviors are designed, NOT executed resolver tests.
No full WAL segments, instrument definition, production normalizer, warm-up/quiet descriptor golden or multi-frame recovery golden is supplied by these five fixtures. Those subsequent assertions remain NOT_IMPLEMENTED/NOT_RUN pending design approval. Content refs are not signatures, an exchange guarantee or storage-fsync proof.
