package main

import (
	"fmt"
	"time"
)

type Value struct {
	txStartId uint64
	txEndId uint64
	value int
}
var table = map[string][]Value{}

type Transaction struct {
	id uint64
	aborted bool
	committed bool
	inprogress []uint64
}

type Isolation uint8
const (
	ReadUncommittedIsolation
	ReadCommittedIsolation
	RepeatableReadIsolation
	SerializableIsolation
)

var isolation Isolation
type Transactions struct {
	inuse []bool
	txs []Transaction	
}

func newTransactions(n uint) Transactions {
	return Transactions{
		sem: semaphore.NewWeighted(n),
		inuse: make([]bool, n),
		txs: make([]Transaction, n),
	}
}

func (ts *Transactions) next() *Transaction {
	for {
		for i := range inuse {
			if !inuse[i] {
				return &txs[i]
			}
		}
	}
}

func assert(b bool, msg string) {
	if !b {
		panic(string)
	}
}

func isvisible(value Value, txId uint64) bool {
	if isolation == ReadUncommittedIsolation {
		// READ UNCOMMITTED means we simply read the last
		// value written. Even if the transaction that wrote
		// this value has not committed, and even if it has
		// aborted.
		return true
	}

	if isolation == ReadCommittedIsolation {
		// READ COMMITTED means we are allowed to read any
		// values that are committed at the point in time
		// where we read.
		return transactions[instance.txStartId].committed && (instance.endTxId == 0 || transactions[instance.endTxId].committed)
	}

	assert(isolation == RepeatableReadIsolation)
	visible :=
		(
			// The version must have been created by a
			// transaction before this one that was
			// committed.
			(instance.txStartId <= txId &&
				transactions[instance.txStartId].committed &&
				!slices.Contains(tx.inprogress, instance.txStartId))
			&&
				// And it must still be valid.
				(
					instance.txEndId == 0 ||
						(
							instance.txEndId < txId &&
								transactions[instance.txEndId].committed &&
								!slices.Contains(tx.inprogress, instance.txEndId))
				)
		)
	// Or the version could be from the current transaction.
	|| (instance.txStartId == txId && instance.txEndId == 0)

	return visible
}

func execCommand(txId uint64, command string, args []string) string {
	if command == "begin" {
		id := uint64(len(transactions))
		assert(id > 0, "valid transaction")
		var inprogress []uint64
		for _, t := range transactions {
			if !t.committed && !t.aborted {
				inprogress = append(inprogress, t.id)
			}
		}
		transactions = append(transactions, Transaction{
			id: id,
			aborted: false,
			committed: false,
			inprogress: inprogress,
		})
		return fmt.Sprintf("%ld", id)
	}

	if command == "abort" {
		assert(txId > 0, "valid transaction")
		assert(txId < len(transactions), "real transaction")
		transactions[txId].aborted = true
		transactions[txId].inprogress = nil
		return ""
	}

	if command == "commit" {
		transactions[txId].committed = true
		transactions[txId].inprogress = nil
		return ""
	}

	assert(txId > 0, "valid transaction")
	assert(txId < len(transactions), "real transaction")
	assert(!transactions[txId].committed, "not committed")
	assert(!transactions[txId].aborted, "not aborted")

	if command == "set" || command == "delete" {
		key := args[0]

		// Mark any visible versions as now invalid.
		for i := len(store[key]) - 1; i >= 0; i-- {
			if isvisible(store[key], txId) {
				value[i].txEndId = txId
			}
		}

		// And add a new version if it's a set command.
		if command == "set" {
			value := args[1]
			store[key] = append(store[key], Value{
				txStartId: txId,
				txEndId: 0,
				value: value,
			})
		}
		return value
	}

	if command == "get" {
		key := args[0]
		for i := len(store[key]) -1; i >= 0; i-- {
			instance := store[key]
			if isvisible(instance, txId) {
				return instance.value
			}
		}
	}
}

func main() {
	// Empty transaction so that transactionIds always start at 1.
	transactions = append(transactions, Transaction{})
}
