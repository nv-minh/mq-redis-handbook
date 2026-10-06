package lab

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const hint = `Redis Cluster is not up. Start both topologies with: make up PROFILE="sentinel cluster"`

// requireClusterProfile fails fast with a clear message when the cluster profile is not running.
func requireClusterProfile(t *testing.T) {
	t.Helper()
	// The hint is printed only when the test fails, next to WaitForPort's error.
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(hint)
		}
	})
	for _, port := range ClusterPorts {
		testkit.WaitForPort(t, fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	}
}

// direct opens a connection to ONE node. Protocol 2 keeps CLUSTER SLOTS replies as plain arrays.
func direct(t *testing.T, port int) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port), Protocol: 2, MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// trackKeys deletes the keys through a cluster client when the test ends (DEL is single-slot, so key by key).
func trackKeys(t *testing.T) func(keys ...string) {
	t.Helper()
	var keys []string
	t.Cleanup(func() {
		cluster := ConnectCluster()
		defer func() { _ = cluster.Close() }()
		for _, k := range keys {
			cluster.Del(context.Background(), k)
		}
	})
	return func(add ...string) { keys = append(keys, add...) }
}

func TestSlotForMatchesClusterKeyslot(t *testing.T) {
	requireClusterProfile(t)
	ctx := context.Background()
	node := direct(t, 7001)
	keys := []string{
		"foo", "bar", "123456789", "user:1000", "", "a",
		// hash tag rules from the cluster spec
		"{user1000}.following", "{user1000}.followers",
		"foo{}{bar}",    // empty tag: the whole key is hashed
		"foo{{bar}}zap", // the tag is "{bar"
		"foo{bar}{zap}", // first tag wins: "bar"
		"{}foo",         // key starting with {} hashes the whole key
		"foo{bar",       // no closing brace: the whole key is hashed
		"foo}bar{",      // closing brace before opening brace
		"{", "}", "{}",
		"{{}}", // tag is "{"
		"{a}", "a{b}c{d}e",
		// non-ASCII: the hash runs over the UTF-8 bytes
		"khóa", "日本語", "{日本}語", "café:{ünï}", "🔑", "k🔑{🔑}",
		// bytes that are not valid UTF-8 (Go strings are byte strings)
		"\xff\xfe", "{\xff}x",
	}
	for _, key := range keys {
		want, err := node.Do(ctx, "CLUSTER", "KEYSLOT", key).Int()
		if err != nil {
			t.Fatalf("CLUSTER KEYSLOT %q: %v", key, err)
		}
		if got := SlotFor(key); got != want {
			t.Errorf("SlotFor(%q) = %d, CLUSTER KEYSLOT = %d", key, got, want)
		}
	}
	// CRC16/XMODEM check value from the cluster spec: CRC16("123456789") = 0x31C3.
	if got, want := SlotFor("123456789"), 0x31c3%16384; got != want {
		t.Errorf("SlotFor(123456789) = %d, want %d", got, want)
	}
}

func TestHashTagRulesFollowTheClusterSpec(t *testing.T) {
	requireClusterProfile(t)
	same := [][2]string{
		{"{user1000}.following", "{user1000}.followers"},
		{"{user1000}.following", "user1000"},
		{"foo{bar}{zap}", "bar"},
		{"foo{{bar}}zap", "{bar"},
	}
	for _, p := range same {
		if SlotFor(p[0]) != SlotFor(p[1]) {
			t.Errorf("SlotFor(%q) != SlotFor(%q), want equal", p[0], p[1])
		}
	}
	// Empty tag, unclosed tag and a leading {} hash the whole key.
	different := [][2]string{{"foo{}{bar}", "bar"}, {"foo{bar", "bar"}, {"{}foo", "foo"}}
	for _, p := range different {
		if SlotFor(p[0]) == SlotFor(p[1]) {
			t.Errorf("SlotFor(%q) == SlotFor(%q), want different", p[0], p[1])
		}
	}
}

func TestKeysWithSameHashTagShareASlot(t *testing.T) {
	requireClusterProfile(t)
	ctx := context.Background()
	node := direct(t, 7001)
	track := trackKeys(t)
	tag := testkit.UniqueName("lab04-tag")
	sameTag := []string{"{" + tag + "}:pending", "{" + tag + "}:processing", "{" + tag + "}:dead"}
	for _, key := range sameTag {
		got, err := node.Do(ctx, "CLUSTER", "KEYSLOT", key).Int()
		if err != nil || got != SlotFor("{"+tag+"}") || SlotFor(key) != SlotFor("{"+tag+"}") {
			t.Fatalf("key %q: KEYSLOT=%d err=%v SlotFor=%d, want %d", key, got, err, SlotFor(key), SlotFor("{"+tag+"}"))
		}
	}
	// Without a tag the same suffixes spread over slots (fixed names, known to differ).
	if SlotFor("{alpha}:pending") == SlotFor("{beta}:pending") || SlotFor("alpha:pending") == SlotFor("alpha:processing") {
		t.Fatal("expected untagged keys to land in different slots")
	}

	// A multi-key command on keys of one slot works through the cluster client.
	cluster := ConnectCluster()
	defer func() { _ = cluster.Close() }()
	track(sameTag...)
	if err := cluster.MSet(ctx, sameTag[0], "1", sameTag[1], "2", sameTag[2], "3").Err(); err != nil {
		t.Fatalf("MSET with one hash tag: %v", err)
	}
	got, err := cluster.MGet(ctx, sameTag...).Result()
	if err != nil || fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("MGET = %v err=%v, want [1 2 3]", got, err)
	}
}

func TestMultiKeyCommandAcrossSlotsFailsWithCrossslot(t *testing.T) {
	requireClusterProfile(t)
	ctx := context.Background()
	track := trackKeys(t)

	prefix := testkit.UniqueName("lab04-cross")
	a, b := prefix+"-a", prefix+"-b"
	for i := 0; SlotFor(a) == SlotFor(b); i++ {
		b = prefix + "-b" + strconv.Itoa(i)
	}
	track(a, b)

	// Send MSET to ONE node over a direct connection: CROSSSLOT is checked before MOVED.
	err := direct(t, 7001).MSet(ctx, a, "1", b, "2").Err()
	if err == nil || !strings.HasPrefix(err.Error(), "CROSSSLOT") {
		t.Fatalf("MSET across slots: err = %v, want a CROSSSLOT error", err)
	}

	// The same keys under one hash tag succeed on the node that owns the slot.
	tag := testkit.UniqueName("lab04-tagged")
	tagged := []string{"{" + tag + "}:a", "{" + tag + "}:b"}
	track(tagged...)
	owner, err := SlotOwner(ctx, SlotFor(tagged[0]))
	if err != nil {
		t.Fatalf("slot owner: %v", err)
	}
	ownerPort := portOf(t, HostAddr(owner))
	ownerNode := direct(t, ownerPort)
	if err := ownerNode.MSet(ctx, tagged[0], "1", tagged[1], "2").Err(); err != nil {
		t.Fatalf("MSET on the owner %s: %v", owner, err)
	}
	if got, _ := ownerNode.MGet(ctx, tagged...).Result(); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("MGET on the owner = %v, want [1 2]", got)
	}

	// Any other node (a master of another shard or a replica) answers MOVED for a write to this slot.
	otherPort := ClusterPorts[0]
	if otherPort == ownerPort {
		otherPort = ClusterPorts[1]
	}
	err = direct(t, otherPort).MSet(ctx, tagged[0], "1", tagged[1], "2").Err()
	if err == nil || !strings.HasPrefix(err.Error(), "MOVED") {
		t.Fatalf("MSET on a non-owner: err = %v, want MOVED", err)
	}
}

func TestClusterClientWritesKeysSpreadOverAllMasters(t *testing.T) {
	requireClusterProfile(t)
	ctx := context.Background()
	track := trackKeys(t)
	// Proves the Dialer: after CLUSTER SLOTS the client dials redis-cluster-N:700N, which must map to localhost.
	cluster := ConnectCluster()
	defer func() { _ = cluster.Close() }()
	prefix := testkit.UniqueName("lab04-spread")
	owners := map[string]bool{}
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("%s:%d", prefix, i)
		track(key)
		if err := cluster.Set(ctx, key, strconv.Itoa(i), 0).Err(); err != nil {
			t.Fatalf("SET %s: %v", key, err)
		}
		owner, err := SlotOwner(ctx, SlotFor(key))
		if err != nil {
			t.Fatalf("slot owner: %v", err)
		}
		owners[owner] = true
	}
	for i := 0; i < 60; i++ {
		got, err := cluster.Get(ctx, fmt.Sprintf("%s:%d", prefix, i)).Result()
		if err != nil || got != strconv.Itoa(i) {
			t.Fatalf("GET %d = %q err=%v", i, got, err)
		}
	}
	if len(owners) != 3 {
		t.Fatalf("keys landed on %d masters (%v), want 3", len(owners), owners)
	}
}

func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, p, ok := strings.Cut(addr, ":")
	if !ok {
		t.Fatalf("no port in %q", addr)
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("port of %q: %v", addr, err)
	}
	return port
}
