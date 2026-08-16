package wal

import (
	"encoding/json"
	"sort"

	"github.com/cockroachdb/errors"
)

// PendingTxn is an unfinished transaction observed by Analyze. The step
// engine will replay it: each step in CompletedSteps is already durably
// applied; the next step to execute is the first INTENT whose StepID is
// not in CompletedSteps. If LastIntent is nil, the transaction crashed
// before any step started — it can be safely aborted.
//
// Prepared is true iff a TXN_PREPARE record was observed for this txn.
// Recovery must NOT redo a transaction without Prepared, because the
// intent set may be torn (a crash between two consecutive Intent calls
// leaves only some intents durable, replaying which would land at an
// intermediate state). Such transactions should be aborted instead.
type PendingTxn struct {
	ID             TxnID
	Op             Op
	Params         []byte // raw JSON; op-specific
	Prepared       bool
	LastIntent     *IntentPayload
	CompletedSteps map[uint32]bool
	PendingIntents []IntentPayload // intents observed in order, including LastIntent
}

// Analysis is the result of scanning a journal for recovery.
type Analysis struct {
	NextTxnID TxnID
	Pending   []PendingTxn // sorted by TxnID ascending
}

// Analyze reads all records and returns the set of transactions that need
// to be replayed or aborted by the step engine.
func Analyze(records []Record) (*Analysis, error) {
	type state struct {
		begin    TxnBeginPayload
		intents  []IntentPayload
		seen     map[uint32]bool // INTENT step IDs already observed
		done     map[uint32]bool
		prepared bool
		finished bool
	}
	txns := map[TxnID]*state{}
	var maxTxn TxnID

	for _, rec := range records {
		switch rec.Type {
		case RecTxnBegin:
			var p TxnBeginPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode TXN_BEGIN")
			}
			if _, exists := txns[p.TxnID]; exists {
				return nil, errors.Errorf("duplicate TXN_BEGIN for txn %d", p.TxnID)
			}
			txns[p.TxnID] = &state{begin: p, seen: map[uint32]bool{}, done: map[uint32]bool{}}
			if p.TxnID > maxTxn {
				maxTxn = p.TxnID
			}
		case RecIntent:
			var p IntentPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode INTENT")
			}
			s, ok := txns[p.TxnID]
			if !ok {
				return nil, errors.Errorf("INTENT for unknown txn %d", p.TxnID)
			}
			// An intent after the txn is finished, or after PREPARE sealed
			// the intent set, breaks the prepare boundary: a crash just
			// before this append would replay a different set than a crash
			// just after it. Reject the stream as a broken writer.
			if s.finished {
				return nil, errors.Errorf("INTENT for finished txn %d", p.TxnID)
			}
			if s.prepared {
				return nil, errors.Errorf("INTENT after TXN_PREPARE for txn %d", p.TxnID)
			}
			// Duplicate step IDs are rejected: STEP_DONE completion is keyed
			// by StepID, so two intents sharing an ID would be marked done
			// together and the second could be skipped on replay.
			if s.seen[p.StepID] {
				return nil, errors.Errorf("duplicate INTENT step %d for txn %d", p.StepID, p.TxnID)
			}
			s.seen[p.StepID] = true
			s.intents = append(s.intents, p)
		case RecStepDone:
			var p StepDonePayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode STEP_DONE")
			}
			s, ok := txns[p.TxnID]
			if !ok {
				return nil, errors.Errorf("STEP_DONE for unknown txn %d", p.TxnID)
			}
			// A STEP_DONE after a terminal record contradicts the stream: the
			// txn already committed or aborted. STEP_DONE after PREPARE is
			// normal (steps are applied post-prepare), so only finished is
			// rejected, mirroring the writer's terminal check.
			if s.finished {
				return nil, errors.Errorf("STEP_DONE for finished txn %d", p.TxnID)
			}
			s.done[p.StepID] = true
		case RecTxnCommit:
			var p TxnEndPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode TXN_COMMIT")
			}
			s, ok := txns[p.TxnID]
			if !ok {
				// No BEGIN for this txn earlier in the stream. This package's
				// checkpoint truncates the whole file, so a journal it produced
				// never carries a COMMIT without its BEGIN; tolerate it
				// defensively (nothing to finish) rather than failing recovery.
				break
			}
			// A COMMIT must seal a fully-completed transaction: it must have
			// been prepared and every sealed intent must have a STEP_DONE.
			// A validly framed but premature COMMIT means a broken writer;
			// erroring (rather than silently dropping the txn from Pending)
			// forces quarantine instead of losing an unfinished operation.
			if !s.prepared {
				return nil, errors.Errorf("TXN_COMMIT for unprepared txn %d", p.TxnID)
			}
			for _, in := range s.intents {
				if !s.done[in.StepID] {
					return nil, errors.Errorf("TXN_COMMIT for txn %d with incomplete step %d", p.TxnID, in.StepID)
				}
			}
			s.finished = true
		case RecTxnAbort:
			var p TxnEndPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode TXN_ABORT")
			}
			if s, ok := txns[p.TxnID]; ok {
				// After PREPARE, recovery promises roll-forward, so a durable
				// ABORT of a prepared txn is a broken writer: a step may have
				// been applied without its STEP_DONE, and finishing the txn
				// would let a checkpoint erase the only replay plan. Error to
				// force quarantine rather than losing partial filesystem state.
				if s.prepared {
					return nil, errors.Errorf("TXN_ABORT for prepared txn %d", p.TxnID)
				}
				s.finished = true
			}
		case RecTxnPrepare:
			var p TxnEndPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode TXN_PREPARE")
			}
			s, ok := txns[p.TxnID]
			if !ok {
				return nil, errors.Errorf("TXN_PREPARE for unknown txn %d", p.TxnID)
			}
			// A PREPARE after the txn already finished (its COMMIT or ABORT is
			// durable), or a second PREPARE, is a broken writer: the record
			// stream contradicts itself (e.g. BEGIN -> ABORT -> PREPARE ->
			// COMMIT records opposite terminal outcomes). Error to force
			// quarantine rather than silently sealing a contradictory txn.
			if s.finished {
				return nil, errors.Errorf("TXN_PREPARE for finished txn %d", p.TxnID)
			}
			if s.prepared {
				return nil, errors.Errorf("duplicate TXN_PREPARE for txn %d", p.TxnID)
			}
			s.prepared = true
		case RecCheckpoint:
			// A CHECKPOINT in the middle of the stream is unusual but
			// possible (a writer that did not truncate after writing it).
			// Pending txns observed before this point are still replayed;
			// the CHECKPOINT only contributes its NextTxnID hint.
			var p CheckpointPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode CHECKPOINT")
			}
			if p.NextTxnID > 0 && p.NextTxnID-1 > maxTxn {
				maxTxn = p.NextTxnID - 1
			}
		default:
			// A CRC-valid frame with an unknown record type cannot be
			// interpreted, so recovering (and later checkpointing) it would
			// erase a record whose semantics we never understood. Reject it
			// so the journal is preserved for quarantine/inspection.
			return nil, errors.Errorf("unknown record type %d", rec.Type)
		}
	}

	a := &Analysis{NextTxnID: maxTxn + 1}
	for id, s := range txns {
		if s.finished {
			continue
		}
		// Copy the completed-steps set so callers can mutate CompletedSteps
		// without corrupting Analyze's internal state.
		done := make(map[uint32]bool, len(s.done))
		for step, ok := range s.done {
			done[step] = ok
		}
		pt := PendingTxn{
			ID:             id,
			Op:             s.begin.Op,
			Params:         s.begin.Params,
			Prepared:       s.prepared,
			CompletedSteps: done,
			PendingIntents: s.intents,
		}
		if n := len(s.intents); n > 0 {
			pt.LastIntent = &s.intents[n-1]
		}
		a.Pending = append(a.Pending, pt)
	}
	sort.Slice(a.Pending, func(i, j int) bool { return a.Pending[i].ID < a.Pending[j].ID })
	return a, nil
}
