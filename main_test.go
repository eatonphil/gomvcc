package main

import (
	"testing"
)

func TestReadUncommitted(t *testing.T) {
	database := newDatabase(10)
	database.defaultIsolation = ReadUncommittedIsolation

	c1 := database.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := database.newConnection()
	c2.mustExecCommand("begin", nil)

	c1.mustExecCommand("set", []string{"x", "hey"})

	// Update is visible to self.
	res := c1.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c1 get x")

	// But since read uncommitted, also available to everyone else.
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")
}

func TestReadCommitted(t *testing.T) {
	database := newDatabase(10)
	database.defaultIsolation = ReadCommittedIsolation

	c1 := database.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := database.newConnection()
	c2.mustExecCommand("begin", nil)

	// Local change is visible locally.
	c1.mustExecCommand("set", []string{"x", "hey"})

	res := c1.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c1 get x")

	// Update not available to this transaction since this is not
	// committed.
	res, err := c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), "cannot get key that does not exist", "c2 get x")

	c1.mustExecCommand("commit", nil)

	// Now that it's been committed, it's visible in c2.
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	c3 := database.newConnection()
	c3.mustExecCommand("begin", nil)

	// Local change is visible locally.
	c3.mustExecCommand("set", []string{"x", "yall"})
	
	res = c3.mustExecCommand("get", []string{"x"})
	assertEq(res, "yall", "c3 get x")

	// But not on the other commit, again.
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	c3.mustExecCommand("abort", nil)

	// And still not, if the other transaction aborted.
	res = c2.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	// And if we delete it, it should show up deleted locally.
	c2.mustExecCommand("delete", []string{"x"})

	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), "cannot get key that does not exist", "c2 get x")
	
	c2.mustExecCommand("commit", nil)

	// It should also show up as deleted in new transactions now
	// that it has been committed.
	c4 := database.newConnection()
	c4.mustExecCommand("begin", nil)

	res, err = c4.execCommand("get", []string{"x"})
	assertEq(res, "", "c4 get x")
	assertEq(err.Error(), "cannot get key that does not exist", "c4 get x")
}

func TestRepeatableRead(t *testing.T) {
	database := newDatabase(10)
	database.defaultIsolation = RepeatableReadIsolation

	c1 := database.newConnection()
	c1.mustExecCommand("begin", nil)

	c2 := database.newConnection()
	c2.mustExecCommand("begin", nil)

	// Local change is visible locally.
	c1.mustExecCommand("set", []string{"x", "hey"})
	res := c1.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c1 get x")

	// Update not available to this transaction since this is not
	// committed.
	res, err := c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), "cannot get key that does not exist", "c2 get x")

	c1.mustExecCommand("commit", nil)

	// Even after committing, it's not visible in an existing
	// transaction.
	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), "cannot get key that does not exist", "c2 get x")

	// But is available in a new transaction.
	c3 := database.newConnection()
	c3.mustExecCommand("begin", nil)

	res = c3.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c3 get x")

	// Local change is visible locally.
	c3.mustExecCommand("set", []string{"x", "yall"})
	res = c3.mustExecCommand("get", []string{"x"})
	assertEq(res, "yall", "c3 get x")

	// But not on the other commit, again.
	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), "cannot get key that does not exist", "c2 get x")

	c3.mustExecCommand("abort", nil)

	// And still not, regardless of abort, because it's an older
	// transaction.
	res, err = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")
	assertEq(err.Error(), "cannot get key that does not exist", "c2 get x")

	// And again still the aborted set is still not on a new
	// transaction.
	c4 := database.newConnection()
	res = c4.mustExecCommand("begin", nil)

	res = c4.mustExecCommand("get", []string{"x"})
	assertEq(res, "hey", "c4 get x")

	c4.mustExecCommand("delete", []string{"x"})
	c4.mustExecCommand("commit", nil)

	// But the delete is visible to new transactions now that this
	// has been committed.
	c5 := database.newConnection()
	res = c5.mustExecCommand("begin", nil)

	res, err = c5.execCommand("get", []string{"x"})
	assertEq(res, "", "c5 get x")
	assertEq(err.Error(), "cannot get key that does not exist", "c5 get x")
}
