package main

import (
	"testing"
)

func TestReadUncommitted(t *testing.T) {
	database := newDatabase(10)

	c1 := database.newConnection()
	c1.execCommand("begin", nil)
	c1.tx.isolation = ReadUncommittedIsolation
	
	c2 := database.newConnection()
	c2.execCommand("begin", nil)
	c2.tx.isolation = ReadUncommittedIsolation

	c1.execCommand("set", []string{"x", "hey"})

	// Update is visible to self.
	res := c1.execCommand("get", []string{"x"})
	assert(res == "hey", "x == " + res)

	// But since read uncommitted, also available to everyone else.
	res = c2.execCommand("get", []string{"x"})
	assert(res == "hey", "x == " + res)
}

func TestReadCommitted(t *testing.T) {
	database := newDatabase(10)

	c1 := database.newConnection()
	c1.execCommand("begin", nil)
	c1.tx.isolation = ReadCommittedIsolation
	
	c2 := database.newConnection()
	c2.execCommand("begin", nil)
	c2.tx.isolation = ReadCommittedIsolation

	c1.execCommand("set", []string{"x", "hey"})
	res := c1.execCommand("get", []string{"x"})
	assert(res == "", "x == " + res)

	// Update not available to this transaction since this is not
	// committed.
	res = c2.execCommand("get", []string{"x"})
	assert(res == "", "x == " + res)

	c1.execCommand("commit", nil)

	// Now that it's been committed, it's visible in c2.
	res = c2.execCommand("get", []string{"x"})
	assert(res == "hey", "x == " + res)

	c3 := database.newConnection()
	c3.execCommand("begin", nil)
	c3.tx.isolation = ReadCommittedIsolation

	// Local change is visible locally.
	c3.execCommand("set", []string{"x", "yall"})
	res = c3.execCommand("get", []string{"x"})
	assert(res == "yall", "x == " + res)

	// But not on the other commit, again.
	res = c2.execCommand("get", []string{"x"})
	assert(res == "hey", "x == " + res)

	c3.execCommand("abort", nil)

	// And still not, if the other transaction aborted.
	res = c2.execCommand("get", []string{"x"})
	assert(res == "hey", "x == hey")
}
