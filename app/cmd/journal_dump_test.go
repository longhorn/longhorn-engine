package cmd

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urfave/cli"

	"github.com/longhorn/go-common-libs/wal"
)

// mustJSON marshals v to JSON; test-only, panics on encoder error.
func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// recBegin builds a TXN_BEGIN record.
func recBegin(t *testing.T, id wal.TxnID, op wal.Op, params []byte) wal.Record {
	return wal.Record{Type: wal.RecTxnBegin, Payload: mustJSON(t, wal.TxnBeginPayload{TxnID: id, Op: op, Params: params})}
}

// recIntent builds an INTENT record.
func recIntent(t *testing.T, id wal.TxnID, step uint32, action wal.Action, args []byte) wal.Record {
	return wal.Record{Type: wal.RecIntent, Payload: mustJSON(t, wal.IntentPayload{TxnID: id, StepID: step, Action: action, Args: args})}
}

// recEnd builds a TXN_PREPARE / TXN_COMMIT / TXN_ABORT record.
func recEnd(t *testing.T, typ wal.RecordType, id wal.TxnID) wal.Record {
	return wal.Record{Type: typ, Payload: mustJSON(t, wal.TxnEndPayload{TxnID: id})}
}

// recStepDone builds a STEP_DONE record.
func recStepDone(t *testing.T, id wal.TxnID, step uint32) wal.Record {
	return wal.Record{Type: wal.RecStepDone, Payload: mustJSON(t, wal.StepDonePayload{TxnID: id, StepID: step})}
}

// recCheckpoint builds a CHECKPOINT record.
func recCheckpoint(t *testing.T, next wal.TxnID) wal.Record {
	return wal.Record{Type: wal.RecCheckpoint, Payload: mustJSON(t, wal.CheckpointPayload{NextTxnID: next})}
}

func TestParamsPreview(t *testing.T) {
	if got := paramsPreview(nil); got != "-" {
		t.Errorf("nil: want %q, got %q", "-", got)
	}
	if got := paramsPreview([]byte("{}")); got != "{}" {
		t.Errorf("short: want %q, got %q", "{}", got)
	}
	long := bytes.Repeat([]byte("a"), 200)
	got := paramsPreview(long)
	if len(got) != 80+3 || !strings.HasSuffix(got, "...") {
		t.Errorf("long: expected 80 chars + ellipsis, got len=%d %q", len(got), got)
	}
}

func TestRecordSummary(t *testing.T) {
	tests := []struct {
		name      string
		rec       wal.Record
		wantTxn   wal.TxnID
		wantParts []string
	}{
		{
			name:      "begin",
			rec:       recBegin(t, 7, wal.OpSnapCreate, []byte(`{"snapshot":"snap-a"}`)),
			wantTxn:   7,
			wantParts: []string{"op=SNAP_CREATE", `params={"snapshot":"snap-a"}`},
		},
		{
			name:      "intent",
			rec:       recIntent(t, 7, 2, wal.ActionCreateHead, []byte(`{"head_name":"volume-head-001.img"}`)),
			wantTxn:   7,
			wantParts: []string{"step=2", "action=CREATE_HEAD", "args="},
		},
		{
			name:      "step_done",
			rec:       recStepDone(t, 7, 3),
			wantTxn:   7,
			wantParts: []string{"step=3"},
		},
		{
			name:      "prepare",
			rec:       recEnd(t, wal.RecTxnPrepare, 7),
			wantTxn:   7,
			wantParts: nil,
		},
		{
			name:      "commit",
			rec:       recEnd(t, wal.RecTxnCommit, 7),
			wantTxn:   7,
			wantParts: nil,
		},
		{
			name:      "abort",
			rec:       recEnd(t, wal.RecTxnAbort, 7),
			wantTxn:   7,
			wantParts: nil,
		},
		{
			name:      "checkpoint",
			rec:       recCheckpoint(t, 42),
			wantTxn:   0,
			wantParts: []string{"next_txn_id=42"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn, sum := recordSummary(tt.rec)
			if txn != tt.wantTxn {
				t.Errorf("txn: want %d, got %d", tt.wantTxn, txn)
			}
			for _, p := range tt.wantParts {
				if !strings.Contains(sum, p) {
					t.Errorf("summary %q missing %q", sum, p)
				}
			}
		})
	}
}

func TestRecordSummaryDecodeError(t *testing.T) {
	r := wal.Record{Type: wal.RecTxnBegin, Payload: []byte("not json")}
	_, sum := recordSummary(r)
	if !strings.Contains(sum, "decode error") {
		t.Errorf("expected decode error message, got %q", sum)
	}
}

func TestResolveJournalPath(t *testing.T) {
	dir := t.TempDir()

	// Directory: returns dir/journal.log even if file does not exist.
	got, err := resolveJournalPath(dir)
	if err != nil {
		t.Fatalf("dir: %v", err)
	}
	if want := filepath.Join(dir, wal.FileName); got != want {
		t.Errorf("dir: want %q, got %q", want, got)
	}

	// File: returns the file path unchanged.
	file := filepath.Join(dir, "custom.log")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	got, err = resolveJournalPath(file)
	if err != nil {
		t.Fatalf("file: %v", err)
	}
	if got != file {
		t.Errorf("file: want %q, got %q", file, got)
	}

	// Missing path: error.
	if _, err := resolveJournalPath(filepath.Join(dir, "nope")); err == nil {
		t.Errorf("missing: expected error")
	}
}

// sampleRecords produces a realistic record set covering: a committed txn,
// a prepared-but-not-committed txn (recovery should REDO), and an
// un-prepared torn-intent txn (recovery should ABORT).
func sampleRecords(t *testing.T) []wal.Record {
	return []wal.Record{
		// Committed txn 1.
		recBegin(t, 1, wal.OpSnapCreate, []byte(`{"snapshot":"a"}`)),
		recIntent(t, 1, 1, wal.ActionCreateHead, []byte(`{"head_name":"h.img"}`)),
		recEnd(t, wal.RecTxnPrepare, 1),
		recStepDone(t, 1, 1),
		recEnd(t, wal.RecTxnCommit, 1),
		// Prepared-but-pending txn 2 (REDO).
		recBegin(t, 2, wal.OpSnapRemove, []byte(`{"name":"a"}`)),
		recIntent(t, 2, 1, wal.ActionRmDisk, []byte(`{"name":"a"}`)),
		recEnd(t, wal.RecTxnPrepare, 2),
		// Un-prepared (torn intent) txn 3 (ABORT).
		recBegin(t, 3, wal.OpSnapRevert, []byte(`{"parent":"p"}`)),
		recIntent(t, 3, 1, wal.ActionCreateHead, []byte(`{"head_name":"h2.img"}`)),
	}
}

func TestDumpTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, wal.FileName)
	if err := os.WriteFile(path, []byte("stub"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	recs := sampleRecords(t)

	var buf bytes.Buffer
	if err := dumpTable(&buf, path, recs, false); err != nil {
		t.Fatalf("dumpTable: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"Journal:",
		path,
		"IDX",
		"TYPE",
		"TXN",
		"SUMMARY",
		"TXN_BEGIN",
		"INTENT",
		"TXN_PREPARE",
		"TXN_COMMIT",
		"STEP_DONE",
		"SNAP_CREATE",
		"SNAP_REMOVE",
		"SNAP_REVERT",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// Verbose mode adds a "payload:" line per record.
	buf.Reset()
	if err := dumpTable(&buf, path, recs, true); err != nil {
		t.Fatalf("dumpTable verbose: %v", err)
	}
	if !strings.Contains(buf.String(), "payload:") {
		t.Errorf("verbose: missing payload line")
	}
}

func TestDumpJSON(t *testing.T) {
	recs := sampleRecords(t)
	var buf bytes.Buffer
	if err := dumpJSON(&buf, recs, false); err != nil {
		t.Fatalf("dumpJSON: %v", err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != len(recs) {
		t.Fatalf("want %d lines, got %d", len(recs), len(lines))
	}
	for i, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("line %d: invalid JSON: %v\n%s", i, err, line)
		}
		if got, ok := obj["idx"].(float64); !ok || int(got) != i {
			t.Errorf("line %d: idx want %d, got %v", i, i, obj["idx"])
		}
		if _, ok := obj["type"].(string); !ok {
			t.Errorf("line %d: missing/invalid type field", i)
		}
		if _, ok := obj["payload"]; ok {
			t.Errorf("line %d: payload should be omitted in non-verbose mode", i)
		}
	}

	// Verbose adds the payload field.
	buf.Reset()
	if err := dumpJSON(&buf, recs[:1], true); err != nil {
		t.Fatalf("dumpJSON verbose: %v", err)
	}
	var obj map[string]any
	if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
		t.Fatalf("verbose JSON: %v", err)
	}
	if _, ok := obj["payload"]; !ok {
		t.Errorf("verbose: missing payload field")
	}
}

func TestPrintAnalysisText(t *testing.T) {
	recs := sampleRecords(t)
	var buf bytes.Buffer
	if err := printAnalysis(&buf, recs, false); err != nil {
		t.Fatalf("printAnalysis: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"Analysis:",
		"next_txn_id",
		"pending",
		// txn 2 is prepared but not committed -> REDO.
		"REDO  (prepared)",
		"txn 2",
		// txn 3 has no PREPARE -> ABORT.
		"ABORT (un-prepared)",
		"txn 3",
		// Each pending txn's intent should appear.
		"step=1 action=RM_DISK",
		"step=1 action=CREATE_HEAD",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// Committed txn 1 must NOT show up as pending.
	if strings.Contains(out, "txn 1 ") {
		t.Errorf("committed txn 1 should not be listed as pending:\n%s", out)
	}
}

func TestPrintAnalysisJSON(t *testing.T) {
	recs := sampleRecords(t)
	var buf bytes.Buffer
	if err := printAnalysis(&buf, recs, true); err != nil {
		t.Fatalf("printAnalysis: %v", err)
	}
	var obj struct {
		Kind      string           `json:"kind"`
		NextTxnID wal.TxnID        `json:"next_txn_id"`
		Pending   []wal.PendingTxn `json:"pending"`
	}
	if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
		t.Fatalf("decode: %v\n%s", err, buf.String())
	}
	if obj.Kind != "analysis" {
		t.Errorf("kind: want analysis, got %q", obj.Kind)
	}
	if len(obj.Pending) != 2 {
		t.Fatalf("want 2 pending (txn 2, txn 3), got %d", len(obj.Pending))
	}
	// Pending is sorted by TxnID ascending.
	if obj.Pending[0].ID != 2 || !obj.Pending[0].Prepared {
		t.Errorf("pending[0]: want txn=2 prepared=true, got txn=%d prepared=%v", obj.Pending[0].ID, obj.Pending[0].Prepared)
	}
	if obj.Pending[1].ID != 3 || obj.Pending[1].Prepared {
		t.Errorf("pending[1]: want txn=3 prepared=false, got txn=%d prepared=%v", obj.Pending[1].ID, obj.Pending[1].Prepared)
	}
}

func TestJournalDumpMissingArg(t *testing.T) {
	app := cli.NewApp()
	ctx := cli.NewContext(app, flag.NewFlagSet("test", flag.ContinueOnError), nil)
	if err := journalDump(ctx); err == nil {
		t.Errorf("expected error when no argument is provided")
	}
}
