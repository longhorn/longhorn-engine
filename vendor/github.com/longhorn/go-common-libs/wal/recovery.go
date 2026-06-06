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
			txns[p.TxnID] = &state{begin: p, done: map[uint32]bool{}}
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
			s.done[p.StepID] = true
		case RecTxnCommit, RecTxnAbort:
			var p TxnEndPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode txn end")
			}
			if s, ok := txns[p.TxnID]; ok {
				s.finished = true
			}
		case RecTxnPrepare:
			var p TxnEndPayload
			if err := json.Unmarshal(rec.Payload, &p); err != nil {
				return nil, errors.Wrap(err, "decode TXN_PREPARE")
			}
			if s, ok := txns[p.TxnID]; ok {
				s.prepared = true
			}
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
		}
	}

	a := &Analysis{NextTxnID: maxTxn + 1}
	for id, s := range txns {
		if s.finished {
			continue
		}
		pt := PendingTxn{
			ID:             id,
			Op:             s.begin.Op,
			Params:         s.begin.Params,
			Prepared:       s.prepared,
			CompletedSteps: s.done,
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
