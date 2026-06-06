package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/go-common-libs/wal"
)

func JournalDumpCmd() cli.Command {
	return cli.Command{
		Name:      "journal-dump",
		Usage:     "Pretty-print the snapshot-chain WAL of a replica directory",
		ArgsUsage: "<replica-dir | journal-file>",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  "json",
				Usage: "Emit one JSON object per record instead of a table",
			},
			cli.BoolFlag{
				Name:  "verbose",
				Usage: "Include raw payload JSON for each record",
			},
			cli.BoolFlag{
				Name:  "no-analysis",
				Usage: "Skip the trailing recovery analysis summary",
			},
		},
		Action: func(c *cli.Context) {
			if err := journalDump(c); err != nil {
				logrus.WithError(err).Fatalf("Error running journal-dump")
			}
		},
	}
}

func journalDump(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("expected exactly one argument: replica directory or journal file path")
	}
	arg := c.Args().First()

	path, err := resolveJournalPath(arg)
	if err != nil {
		return err
	}

	records, err := wal.ScanFile(path)
	if err != nil {
		return fmt.Errorf("scan %s: %w", path, err)
	}

	if c.Bool("json") {
		if err := dumpJSON(os.Stdout, records, c.Bool("verbose")); err != nil {
			return err
		}
	} else {
		if err := dumpTable(os.Stdout, path, records, c.Bool("verbose")); err != nil {
			return err
		}
	}

	if c.Bool("no-analysis") {
		return nil
	}
	return printAnalysis(os.Stdout, records, c.Bool("json"))
}

// resolveJournalPath accepts either the replica directory or the journal
// file itself and returns the path to the journal file.
func resolveJournalPath(arg string) (string, error) {
	st, err := os.Stat(arg)
	if err != nil {
		return "", err
	}
	if st.IsDir() {
		return filepath.Join(arg, wal.FileName), nil
	}
	return arg, nil
}

// recordSummary builds a one-line summary of a record's payload.
func recordSummary(r wal.Record) (txnID wal.TxnID, summary string) {
	switch r.Type {
	case wal.RecTxnBegin:
		var p wal.TxnBeginPayload
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return 0, fmt.Sprintf("decode error: %v", err)
		}
		return p.TxnID, fmt.Sprintf("op=%s params=%s", p.Op, paramsPreview(p.Params))
	case wal.RecIntent:
		var p wal.IntentPayload
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return 0, fmt.Sprintf("decode error: %v", err)
		}
		return p.TxnID, fmt.Sprintf("step=%d action=%s args=%s", p.StepID, p.Action, paramsPreview(p.Args))
	case wal.RecStepDone:
		var p wal.StepDonePayload
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return 0, fmt.Sprintf("decode error: %v", err)
		}
		return p.TxnID, fmt.Sprintf("step=%d", p.StepID)
	case wal.RecTxnPrepare, wal.RecTxnCommit, wal.RecTxnAbort:
		var p wal.TxnEndPayload
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return 0, fmt.Sprintf("decode error: %v", err)
		}
		return p.TxnID, ""
	case wal.RecCheckpoint:
		var p wal.CheckpointPayload
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			return 0, fmt.Sprintf("decode error: %v", err)
		}
		return 0, fmt.Sprintf("next_txn_id=%d", p.NextTxnID)
	}
	return 0, fmt.Sprintf("payload=%d bytes", len(r.Payload))
}

func paramsPreview(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	const maxLen = 80
	s := string(b)
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

func dumpTable(w io.Writer, path string, records []wal.Record, verbose bool) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "Journal: %s\n", path)
	fmt.Fprintf(w, "Size:    %d bytes, %d records\n\n", st.Size(), len(records))

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "IDX\tTYPE\tTXN\tSUMMARY")
	for i, r := range records {
		txn, sum := recordSummary(r)
		txnStr := "-"
		if txn != 0 || r.Type == wal.RecTxnBegin {
			txnStr = fmt.Sprintf("%d", txn)
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", i, r.Type, txnStr, sum)
		if verbose {
			fmt.Fprintf(tw, "\t\t\t  payload: %s\n", string(r.Payload))
		}
	}
	return tw.Flush()
}

func dumpJSON(w io.Writer, records []wal.Record, verbose bool) error {
	enc := json.NewEncoder(w)
	for i, r := range records {
		txn, sum := recordSummary(r)
		obj := map[string]any{
			"idx":     i,
			"type":    r.Type.String(),
			"txn":     txn,
			"summary": sum,
		}
		if verbose {
			obj["payload"] = json.RawMessage(r.Payload)
		}
		if err := enc.Encode(obj); err != nil {
			return err
		}
	}
	return nil
}

func printAnalysis(w io.Writer, records []wal.Record, asJSON bool) error {
	a, err := wal.Analyze(records)
	if err != nil {
		return fmt.Errorf("analyze: %w", err)
	}
	if asJSON {
		obj := map[string]any{
			"kind":        "analysis",
			"next_txn_id": a.NextTxnID,
			"pending":     a.Pending,
		}
		return json.NewEncoder(w).Encode(obj)
	}

	fmt.Fprintf(w, "\nAnalysis:\n")
	fmt.Fprintf(w, "  next_txn_id : %d\n", a.NextTxnID)
	fmt.Fprintf(w, "  pending     : %d\n", len(a.Pending))
	for _, pt := range a.Pending {
		status := "ABORT (un-prepared)"
		if pt.Prepared {
			status = "REDO  (prepared)"
		}
		fmt.Fprintf(w, "    txn %d  op=%s  intents=%d  applied=%d  %s\n",
			pt.ID, pt.Op, len(pt.PendingIntents), len(pt.CompletedSteps), status)
		for _, in := range pt.PendingIntents {
			done := " "
			if pt.CompletedSteps[in.StepID] {
				done = "x"
			}
			fmt.Fprintf(w, "      [%s] step=%d action=%s\n", done, in.StepID, in.Action)
		}
	}
	return nil
}
