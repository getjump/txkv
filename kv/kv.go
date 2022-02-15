package kv

type State struct {
	KV map[string]string

	DeletedKeys map[string]bool

	PrevState *State
}

type KV struct {
	State *State

	LastTransaction *Transaction
}

func NewKV() *KV {
	kv := &KV{State: &State{KV: make(map[string]string), DeletedKeys: make(map[string]bool)}}
	return kv
}

func (kv *KV) Begin() {
	tx := &Transaction{}

	if kv.LastTransaction != nil {
		// Nested transaction
		tx.PrevTransaction = kv.LastTransaction
	}

	kv.LastTransaction = tx

	state := &State{PrevState: kv.State, KV: make(map[string]string), DeletedKeys: make(map[string]bool)}
	kv.State = state
}

func (kv *KV) AppendOperation(op AtomicOperation) {
	if kv.LastTransaction != nil {
		kv.LastTransaction.Operations = append(kv.LastTransaction.Operations, op)
	}
}

func (kv *KV) Count(needle string) int {
	foundKeys := make(map[string]bool)

	result := 0

	state := kv.State

	isDeleted := make(map[string]bool)

	for state != nil {
		for key, _ := range state.DeletedKeys {
			isDeleted[key] = true
		}

		for key, value := range state.KV {
			if _, found := foundKeys[key]; found {
				continue
			}

			foundKeys[key] = true

			if value == needle {
				if _, ok := isDeleted[key]; !ok {
					result += 1
				}
			}
		}

		state = state.PrevState
	}

	return result
}

func (kv *KV) Get(lookupKey string) (string, bool) {
	state := kv.State

	isDeleted := false

	for state != nil {
		if _, ok := state.DeletedKeys[lookupKey]; ok {
			isDeleted = true
		}

		for key, value := range state.KV {
			if key == lookupKey && !isDeleted {
				if _, ok := state.DeletedKeys[lookupKey]; ok {
					return "", false
				}
				return value, true
			}
		}

		state = state.PrevState
	}

	return "", false
}

func (kv *KV) Commit() bool {
	if kv.LastTransaction == nil {
		return false
	}

	if kv.State.PrevState != nil {
		kv.State = kv.State.PrevState
	}

	for _, op := range (*kv.LastTransaction).Operations {
		op.Apply(kv)
	}

	if kv.LastTransaction.PrevTransaction != nil {
		kv.LastTransaction = kv.LastTransaction.PrevTransaction
	} else {
		kv.LastTransaction = nil
	}

	return true
}

func (kv *KV) Rollback() bool {
	if kv.LastTransaction != nil {
		if kv.LastTransaction.PrevTransaction != nil {
			kv.LastTransaction = kv.LastTransaction.PrevTransaction
		} else {
			kv.LastTransaction = nil
		}

		if kv.State.PrevState != nil {
			kv.State = kv.State.PrevState
		}

		return true
	}

	return false
}

type AtomicOperation interface {
	Apply(kv *KV)
}

type SetOperation struct {
	Key   string
	Value string
}
func (op *SetOperation) Apply(kv *KV) {
	kv.State.KV[op.Key] = op.Value
}

type DeleteOperation struct {
	Key string
}
func (op *DeleteOperation) Apply(kv *KV) {
	kv.State.DeletedKeys[op.Key] = true
	delete(kv.State.KV, op.Key)
}

type Transaction struct {
	Operations []AtomicOperation

	PrevTransaction *Transaction
}