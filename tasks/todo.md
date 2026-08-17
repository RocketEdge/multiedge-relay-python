# tasks/todo.md — Sealed Mode (F3) implementation

Plan: C:\Users\jirip\.claude\plans\implement-in-multiedge-signal-distributed-pillow.md
Decisions locked: Ed25519+ML-DSA-65 dual signatures; sealed ⊕ compliance_profile mutually exclusive (422).

## Phase A — SDK crypto core (this repo)
- [ ] A1 Optional extra `sealed = ["cryptography>=47"]` + dev dep; `sealed/` package skeleton with guarded import; new exceptions (SealedError, UnsealError, SealedKeyError, KeyPinningError, NotARecipientError); test_sealed_import.py
- [ ] A2 keys.py: RecipientKeypair/SenderKeypair generate/save/load/public_bundle/fingerprint; canonical_json + bundle_fingerprint; tests
- [ ] A3 core.py: seal()/unseal() per wire spec v1 (AAD, hybrid X25519+ML-KEM-768, HKDF transcript, dual sig, downgrade rejection); tests incl. tamper/substitution/vector fixture/hypothesis
- [ ] A4 Sealer/Unsealer wrappers; add ReceivedSignal.client_signal_id (additive)

## Phase B — C# relay (c:\src\RocketEdge.com\multiedge-signal-relay)
- [ ] B1 StrategyEntity.Sealed + SealedKeyEntity + migration AddSealedMode; CreateStrategy sealed flag + mutual-exclusion rule
- [ ] B2 SealedEnvelopeValidator (Domain, structural, 256 KiB)
- [ ] B3 Publish sealed branch (skip schema/scan)
- [ ] B4 SealedKeyEndpoints (5 routes) + isolation tests
- [ ] B5 Entitlement gate: reject field policy on sealed
- [ ] B6 Delivery/catch-up sealed pass-through (skip FieldFilter; throw on sealed+field-policy)
- [ ] B7 OpenAPI both copies

## Phase C — SDK integration (this repo)
- [ ] C1 prepare_signal(sealer=) + publisher/async param; DLQ holds ciphertext
- [ ] C2 registry.py: Sealer.from_relay/Unsealer.from_relay + fingerprint recompute + pinning; register helpers; fake_relay routes
- [ ] C3 subscriber unsealer= in _deliver; webhook verify_signature(unsealer=)
- [ ] C4 CLI: multiedge sealed keygen|fingerprint|register
- [ ] C5 0.4.0 release chores: version sync, CHANGELOG, README section + ToC, examples

## Phase D — docs/governance/websites
- [ ] D1 ADR 0004 sealed-mode-client-side-crypto (relay repo)
- [ ] D2 relay repo: mvp-deltas F3, architecture.md, changes.md, CLAUDE.md + copilot sync
- [ ] D3 python repo: CLAUDE.md + copilot sync
- [ ] D4 website: index FAQ, pricing, product section, docs/sealed.astro; astro build green
- [ ] D5 c:\!\products.html from live rocketedge.com/products raw HTML (Security card + sealed explainer only)

## Verification gates
- Python: uv run pytest / mypy / ruff check . / black --check .
- Relay: dotnet test MultiEdge.Relay.slnx --filter "Category!=LiveAzure"; dotnet format --verify-no-changes; website npm run build + node --test
