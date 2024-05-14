package main

import (
	"fmt"
	"slices"
	"sync"
	"net/http"
	"log"
)

func assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

type Value struct {
	txStartId uint64
	txEndId uint64
	value string
}

type TransactionState uint8
const (
	InProgressTransaction TransactionState = iota
	AbortedTransaction
	CommittedTransaction
)

type Isolation uint8
const (
	ReadUncommittedIsolation Isolation = iota
	ReadCommittedIsolation
	RepeatableReadIsolation
	SerializableIsolation
)

type Transaction struct {
	isolation Isolation
	id uint64
	inprogress []uint64
}

type Database struct {
	mu sync.Mutex
	txs chan *Transaction

	store map[string][]Value
	history map[uint64]TransactionState
	nextTransactionId uint64
	inprogress []uint64
}

func newDatabase(n int) Database {
	txs := make(chan *Transaction, n)
	for i := 0; i < n; i++ {
		txs <- &Transaction{}
	}

	return Database{
		txs: txs,
		mu: sync.Mutex{},
		store: map[string][]Value{},
		history: map[uint64]TransactionState{},
		nextTransactionId: 1,
		inprogress: []uint64{},
	}
}

func (d *Database) markAndReleaseTransaction(t *Transaction, state TransactionState) {
	d.mu.Lock()
	d.history[t.id] = state
	d.mu.Unlock()
	d.releaseTransaction(t)
}

func (d *Database) transactionState(txId uint64) TransactionState {
	d.mu.Lock()
	s := d.history[txId]
	d.mu.Unlock()
	return s
}

func (d *Database) releaseTransaction(t *Transaction) {
	// Remove transaction from inprogress list.
	d.doForInprogress(func (txId uint64, i int) {
		if txId == t.id {
			d.inprogress[i] = d.inprogress[len(d.inprogress) - 1]
			d.inprogress = d.inprogress[:len(d.inprogress) - 1]
		}
	})
	
	// Reset state.
	t.id = 0
	t.inprogress = nil

	d.txs <- t
}

func (d *Database) nextFreeTransaction() *Transaction {
	t := <-d.txs

	// Assign and increment transaction id.
	d.mu.Lock()
	t.id = d.nextTransactionId
	d.history[t.id] = InProgressTransaction
	d.nextTransactionId++
	d.inprogress = append(d.inprogress, t.id)
	d.mu.Unlock()

	return t
}

func (d *Database) doForInprogress(do func (uint64, int)) {
	d.mu.Lock()
	for i, txId := range d.inprogress {
		do(txId, i)
	}
	d.mu.Unlock()
}

func (d *Database) assertValidTransaction(t *Transaction) {
	assert(t.id > 0, "valid id")
	assert(d.transactionState(t.id) == InProgressTransaction, "in progress")
}

func (d *Database) isvisible(t *Transaction, value Value) bool {
	if t.isolation == ReadUncommittedIsolation {
		// READ UNCOMMITTED means we simply read the last
		// value written. Even if the transaction that wrote
		// this value has not committed, and even if it has
		// aborted.
		return true
	}

	if t.isolation == ReadCommittedIsolation {
		// READ COMMITTED means we are allowed to read any
		// values that are committed at the point in time
		// where we read.
		return d.transactionState(value.txStartId) == CommittedTransaction &&
			(value.txEndId == 0 || d.transactionState(value.txEndId) == CommittedTransaction)
	}

	assert(t.isolation == RepeatableReadIsolation, "is repeatable read")
	return ((
		// The version must have been committed, and committed by a transaction
		// that was not in progress when this transaction started.
		(value.txStartId <= t.id &&
			d.transactionState(value.txStartId) == CommittedTransaction &&
			!slices.Contains(t.inprogress, value.txStartId)) &&

			// And it must still be valid. Or deleted by a
			// transaction that committed, and by a
			// transaction that was not in progress when
			// this transaction started.
			(
				value.txEndId == 0 ||
					(
						value.txEndId < t.id &&
							d.transactionState(value.txEndId) == CommittedTransaction &&
							!slices.Contains(t.inprogress, value.txEndId)))) ||

		// Or the version could be from the current transaction and valid.
		(value.txStartId == t.id && value.txEndId == 0))
}

type Connection struct {
	tx *Transaction
	database *Database
}

func (c Connection) execCommand(command string, args []string) string {
	if command == "begin" {
		c.tx = c.database.nextFreeTransaction()
		c.database.assertValidTransaction(c.tx)
		assert(c.tx.id > 0, "valid transaction")
		c.database.doForInprogress(func (txId uint64, _ int) {
			if txId != c.tx.id {
				c.tx.inprogress = append(c.tx.inprogress, txId)
			}
		})
		return fmt.Sprintf("%ld", c.tx.id)
	}

	if command == "abort" {
		c.database.assertValidTransaction(c.tx)
		c.database.markAndReleaseTransaction(c.tx, AbortedTransaction)
		return ""
	}

	if command == "commit" {
		c.database.assertValidTransaction(c.tx)
		c.database.markAndReleaseTransaction(c.tx, CommittedTransaction)
		return ""
	}

	if command == "set" || command == "delete" {
		c.database.assertValidTransaction(c.tx)

		key := args[0]

		// Mark any visible versions as now invalid.
		for i := len(c.database.store[key]) - 1; i >= 0; i-- {
			value := &c.database.store[key][i]
			if c.database.isvisible(c.tx, *value) {
				value.txEndId = c.tx.id
			}
		}

		// And add a new version if it's a set command.
		if command == "set" {
			value := args[1]
			c.database.store[key] = append(c.database.store[key], Value{
				txStartId: c.tx.id,
				txEndId: 0,
				value: value,
			})

			return value
		}

		return ""
	}

	if command == "get" {
		c.database.assertValidTransaction(c.tx)

		key := args[0]
		for i := len(c.database.store[key]) -1; i >= 0; i-- {
			value := c.database.store[key][i]
			if c.database.isvisible(c.tx, value) {
				return value.value
			}
		}
	}

	assert(false, "no such command")
	return ""
}

func main() {
	database := newDatabase(10)

	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		c := Connection{
			tx: nil,
			database: &database,
		}
		res := c.execCommand("get", []string{"x"})
		fmt.Fprintf(w, "%s", res)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
