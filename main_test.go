package main

import (
	"fmt"
	"testing"
)

func assertEq[C comparable](a C, b C, prefix string) {
	if a != b {
		panic(fmt.Sprintf("%s '%v' != '%v'", prefix, a, b))
	}
}

func TestReadUncommitted(t *testing.T) {
	database := newDatabase(10)
	database.defaultIsolation = ReadUncommittedIsolation

	c1 := database.newConnection()
	c1.execCommand("begin", nil)

	c2 := database.newConnection()
	c2.execCommand("begin", nil)

	c1.execCommand("set", []string{"x", "hey"})

	// Update is visible to self.
	res := c1.execCommand("get", []string{"x"})
	assertEq(res, "hey", "c1 get x")

	// But since read uncommitted, also available to everyone else.
	res = c2.execCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")
}

func TestReadCommitted(t *testing.T) {
	database := newDatabase(10)
	database.defaultIsolation = ReadCommittedIsolation

	c1 := database.newConnection()
	c1.execCommand("begin", nil)

	c2 := database.newConnection()
	c2.execCommand("begin", nil)

	// Local change is visible locally.
	c1.execCommand("set", []string{"x", "hey"})
	res := c1.execCommand("get", []string{"x"})
	assertEq(res, "hey", "c1 get x")

	// Update not available to this transaction since this is not
	// committed.
	res = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")

	c1.execCommand("commit", nil)

	// Now that it's been committed, it's visible in c2.
	res = c2.execCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	c3 := database.newConnection()
	c3.execCommand("begin", nil)

	// Local change is visible locally.
	c3.execCommand("set", []string{"x", "yall"})
	res = c3.execCommand("get", []string{"x"})
	assertEq(res, "yall", "c3 get x")

	// But not on the other commit, again.
	res = c2.execCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	c3.execCommand("abort", nil)

	// And still not, if the other transaction aborted.
	res = c2.execCommand("get", []string{"x"})
	assertEq(res, "hey", "c2 get x")

	// And if we delete it, it should show up deleted locally.
	c2.execCommand("delete", []string{"x"})
	res = c2.execCommand("get", []string{"x"})
	assertEq(res, "", "c2 get x")

	c2.execCommand("commit", nil)

	// It should also show up as deleted in new transactions now
	// that it has been committed.
	c4 := database.newConnection()
	res = c4.execCommand("begin", nil)

	res = c4.execCommand("get", []string{"x"})
	assertEq(res, "", "c4 get x")
}
