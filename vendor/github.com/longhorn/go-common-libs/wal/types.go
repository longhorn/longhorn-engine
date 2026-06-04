package wal

// Op identifies a high-level snapshot chain operation.
type Op string

const (
	OpSnapCreate     Op = "SNAP_CREATE"
	OpSnapRevert     Op = "SNAP_REVERT"
	OpSnapRemove     Op = "SNAP_REMOVE"
	OpSnapRemoveMark Op = "SNAP_REMOVE_MARK"
	OpSnapCoalesce   Op = "SNAP_COALESCE"
	OpSnapPrune      Op = "SNAP_PRUNE"
	OpSnapReplace    Op = "SNAP_REPLACE"
	OpHeadDelete     Op = "HEAD_DELETE"
)

// Action identifies a concrete idempotent filesystem step performed by the
// step engine. Each Action corresponds to one Apply implementation that the
// recovery path can replay.
type Action string

const (
	ActionCreateHead       Action = "CREATE_HEAD"
	ActionLinkAsSnapshot   Action = "LINK_AS_SNAPSHOT"
	ActionUpdateVolumeMeta Action = "UPDATE_VOLUME_META"
	ActionUpdateSnapMeta   Action = "UPDATE_SNAP_META"
	ActionDeleteOldHead    Action = "DELETE_OLD_HEAD"
	ActionRmDisk           Action = "RM_DISK"
	ActionMarkRemoved      Action = "MARK_REMOVED"
	ActionCoalesce         Action = "COALESCE"
	ActionPrune            Action = "PRUNE"
	ActionReplace          Action = "REPLACE"
)

// RecordType is the on-disk record kind. Stable values: do not renumber.
type RecordType uint16

const (
	RecTxnBegin   RecordType = 1
	RecIntent     RecordType = 2
	RecStepDone   RecordType = 3
	RecTxnCommit  RecordType = 4
	RecTxnAbort   RecordType = 5
	RecCheckpoint RecordType = 6
	// RecTxnPrepare marks the boundary between writing intents and
	// applying steps. Recovery only redoes a transaction that has a
	// durable PREPARE; otherwise the intent set might be torn (a
	// crash between two Intent appends) and replaying it would land
	// the on-disk state at an intermediate, inconsistent point.
	RecTxnPrepare RecordType = 7
)

func (t RecordType) String() string {
	switch t {
	case RecTxnBegin:
		return "TXN_BEGIN"
	case RecIntent:
		return "INTENT"
	case RecStepDone:
		return "STEP_DONE"
	case RecTxnCommit:
		return "TXN_COMMIT"
	case RecTxnAbort:
		return "TXN_ABORT"
	case RecCheckpoint:
		return "CHECKPOINT"
	case RecTxnPrepare:
		return "TXN_PREPARE"
	default:
		return "UNKNOWN"
	}
}

// TxnID is a monotonically increasing transaction identifier scoped to the
// journal file. It resets when the journal is checkpointed/truncated.
type TxnID uint64

// Record is the in-memory representation of a journal entry. Payload is
// JSON-encoded for inspectability; the surrounding frame is binary with a
// CRC so torn tails can be detected.
type Record struct {
	Type    RecordType
	Payload []byte // raw JSON; decode with TxnBeginPayload etc.
}

// TxnBeginPayload is the payload of a RecTxnBegin record.
type TxnBeginPayload struct {
	TxnID  TxnID  `json:"txn_id"`
	Op     Op     `json:"op"`
	Params []byte `json:"params,omitempty"` // op-specific JSON
}

// IntentPayload is the payload of a RecIntent record.
type IntentPayload struct {
	TxnID  TxnID  `json:"txn_id"`
	StepID uint32 `json:"step_id"`
	Action Action `json:"action"`
	Args   []byte `json:"args,omitempty"` // action-specific JSON
}

// StepDonePayload is the payload of a RecStepDone record.
type StepDonePayload struct {
	TxnID  TxnID  `json:"txn_id"`
	StepID uint32 `json:"step_id"`
}

// TxnEndPayload is the payload of RecTxnCommit and RecTxnAbort records.
type TxnEndPayload struct {
	TxnID TxnID `json:"txn_id"`
}

// CheckpointPayload is the payload of a RecCheckpoint record.
type CheckpointPayload struct {
	NextTxnID TxnID `json:"next_txn_id"`
}
