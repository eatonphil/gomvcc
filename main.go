package main

import (
	"fmt"
	"slices"
	"sync"
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
	defaultIsolation Isolation

	// Must be accessed via mutex.
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
		mu: sync.Mutex{},
		txs: txs,
		defaultIsolation: ReadCommittedIsolation,

		store: map[string][]Value{},
		history: map[uint64]TransactionState{},
		nextTransactionId: 1,
		inprogress: []uint64{},
	}
}

func (d *Database) completeTransaction(t *Transaction, state TransactionState) {
	// Update history.
	d.mu.Lock()
	d.history[t.id] = state
	d.mu.Unlock()

	// Remove transaction from inprogress list.
	d.doForInprogress(func (txId uint64, i int) {
		if txId == t.id {
			d.inprogress[i] = d.inprogress[len(d.inprogress) - 1]
			d.inprogress = d.inprogress[:len(d.inprogress) - 1]
		}
	})

	// Reset transaction state.
	t.id = 0
	t.inprogress = nil

	// Add back to the pool.
	d.txs <- t
}

func (d *Database) transactionState(txId uint64) TransactionState {
	d.mu.Lock()
	s := d.history[txId]
	d.mu.Unlock()
	return s
}

func (d *Database) nextFreeTransaction() *Transaction {
	t := <-d.txs

	// Assign and increment transaction id.
	d.mu.Lock()
	t.isolation = d.defaultIsolation
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
	// READ UNCOMMITTED means we simply read the last value
	// written. Even if the transaction that wrote this value has
	// not committed, and even if it has aborted.
	if t.isolation == ReadUncommittedIsolation {
		return true
	}

	// READ COMMITTED means we are allowed to read any values that
	// are committed at the point in time where we read.
	if t.isolation == ReadCommittedIsolation {
		// If the value was created by a transaction that is
		// not committed, and not this current transaction,
		// it's no good.
		if d.transactionState(value.txStartId) != CommittedTransaction &&
			value.txStartId != t.id {
			return false
		}

		// If the value was deleted in this transaction, it's no good.
		if value.txEndId == t.id {
			return false
		}

		// Or if the value was deleted in some other committed
		// transaction, it's no good.
		if d.transactionState(value.txEndId) == CommittedTransaction {
			return false
		}

		// Otherwise the value is good.
		return true
	}

	// REPEATABLE READ further restricts READ COMMITTED so only
	// versions from transactions that completed before this one
	// started are visible.
	assert(t.isolation == RepeatableReadIsolation, "is repeatable read")
	// Ignore values from transactions started after this one.
	if value.txStartId > t.id {
		return false
	}

	// Ignore values created from transactions in progress when
	// this one started.
	if slices.Contains(t.inprogress, value.txStartId) {
		return false
	}

	// If the value was created by a transaction that is not
	// committed, and not this current transaction, it's no good.
	if d.transactionState(value.txStartId) != CommittedTransaction &&
		value.txStartId != t.id {
		return false
	}

	// If the value was deleted in this transaction, it's no good.
	if value.txEndId == t.id {
		return false
	}

	// Or if the value was deleted in some other committed
	// transaction that started before this one, it's no good.
	if value.txEndId < t.id &&
		d.transactionState(value.txEndId) == CommittedTransaction &&
		!slices.Contains(t.inprogress, value.txEndId) {
		return false
	}

	return true
}

type Connection struct {
	tx *Transaction
	database *Database
}

func (c *Connection) execCommand(command string, args []string) string {
	fmt.Println(command, args)

	if command == "begin" {
		c.tx = c.database.nextFreeTransaction()
		c.database.assertValidTransaction(c.tx)
		assert(c.tx.id > 0, "valid transaction")
		c.database.doForInprogress(func (txId uint64, _ int) {
			if txId != c.tx.id {
				c.tx.inprogress = append(c.tx.inprogress, txId)
			}
		})
		return fmt.Sprintf("%d", c.tx.id)
	}

	if command == "abort" {
		c.database.assertValidTransaction(c.tx)
		c.database.completeTransaction(c.tx, AbortedTransaction)
		c.tx = nil
		return ""
	}

	if command == "commit" {
		c.database.assertValidTransaction(c.tx)
		c.database.completeTransaction(c.tx, CommittedTransaction)
		c.tx = nil
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
			fmt.Println(value, c.tx)
			if c.database.isvisible(c.tx, value) {
				return value.value
			}
		}

		return ""
	}

	assert(false, "no such command")
	return ""
}

func (d *Database) newConnection() *Connection {
	return &Connection{
		database: d,
		tx: nil,
	}
}
