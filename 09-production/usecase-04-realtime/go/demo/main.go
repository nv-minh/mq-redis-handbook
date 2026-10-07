// Demo realtime: hai instance server WebSocket dùng chung một Redis (đại diện cho hai máy), ba client,
// một message đi từ instance này sang instance kia qua Redis Pub/Sub, và bảng xếp hạng trên sorted set.
// Mọi channel và key nằm dưới một prefix riêng và bị xóa ở cuối.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	lab "github.com/nv-minh/mq-redis-handbook/09-production/usecase-04-realtime/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

// escapeGlob escape các ký tự glob của MATCH: \ * ? [ ].
func escapeGlob(text string) string {
	return regexp.MustCompile(`[\\*?\[\]]`).ReplaceAllString(text, `\$0`)
}

func deleteByPrefix(ctx context.Context, rdb *redis.Client, prefix string) error {
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, escapeGlob(prefix)+"*", 200).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// peer là một client WebSocket của demo, gom message nhận được và in chúng ra.
type peer struct {
	label string
	conn  *websocket.Conn
	done  chan struct{}

	mu       sync.Mutex
	received []lab.ChatMessage
}

func (p *peer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.received)
}

func join(ctx context.Context, s *lab.Server, label, room string) (*peer, error) {
	conn, response, err := websocket.Dial(ctx, s.WSURL(room, label), nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("client %s vào phòng %s: %w", label, room, err)
	}
	p := &peer{label: label, conn: conn, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var message lab.ChatMessage
			if json.Unmarshal(data, &message) != nil {
				continue
			}
			p.mu.Lock()
			p.received = append(p.received, message)
			p.mu.Unlock()
			fmt.Printf("  [%s] nhận từ %s trong phòng %s: %q\n", label, message.From, message.Room, message.Text)
		}
	}()
	return p, nil
}

func send(ctx context.Context, p *peer, text string) error {
	payload, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return err
	}
	return p.conn.Write(ctx, websocket.MessageText, payload)
}

// until poll điều kiện với timeout 5 s.
func until(what string, fn func() bool) error {
	deadline := time.Now().Add(5 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			return fmt.Errorf("hết 5s khi chờ: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func numSub(ctx context.Context, rdb *redis.Client, channel string) int64 {
	counts, err := rdb.PubSubNumSub(ctx, channel).Result()
	if err != nil {
		return -1
	}
	return counts[channel]
}

func chatDemo(ctx context.Context, rdb *redis.Client, prefix string, a, b *lab.Server, peers *[]*peer) error {
	fmt.Println("== 1. Chat: message đi từ instance B sang client của instance A ==")
	channel := lab.RoomChannel(prefix, "general")
	alice, err := join(ctx, a, "alice", "general")
	if err != nil {
		return err
	}
	*peers = append(*peers, alice)
	bob, err := join(ctx, b, "bob", "general")
	if err != nil {
		return err
	}
	*peers = append(*peers, bob)
	carol, err := join(ctx, b, "carol", "random")
	if err != nil {
		return err
	}
	*peers = append(*peers, carol)
	fmt.Printf("alice ở instance A (%s), bob và carol ở instance B (%s).\n", a.URL(), b.URL())
	fmt.Println("alice và bob ở phòng general, carol ở phòng random.")

	// Sẵn sàng một cách xác định: đợi cả hai instance subscribe channel của phòng.
	if err := until("hai instance subscribe channel của phòng", func() bool { return numSub(ctx, rdb, channel) == 2 }); err != nil {
		return err
	}
	fmt.Println("PUBSUB NUMSUB room:general = 2: cả hai instance đã subscribe.")

	fmt.Println(`bob gửi "chào mọi người" qua instance B:`)
	if err := send(ctx, bob, "chào mọi người"); err != nil {
		return err
	}
	if err := until("alice và bob nhận message", func() bool { return alice.count() >= 1 && bob.count() >= 1 }); err != nil {
		return err
	}
	if err := send(ctx, carol, "tôi ở phòng khác"); err != nil {
		return err
	}
	if err := until("carol nhận message của phòng random", func() bool { return carol.count() >= 1 }); err != nil {
		return err
	}
	fmt.Printf("alice nhận %d bản, bob (người gửi) nhận %d bản, carol chỉ nhận message của phòng mình (%d bản).\n", alice.count(), bob.count(), carol.count())
	fmt.Println("Người gửi cũng nhận lại message của mình đúng một lần: instance không giao cục bộ, mọi message đều đi qua Redis.")
	fmt.Println()
	return nil
}

func call(ctx context.Context, method, url, body string) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, data, err
}

func leaderboardDemo(ctx context.Context, a, b *lab.Server) error {
	fmt.Println("== 2. Bảng xếp hạng: POST lên instance A, GET top từ instance B ==")
	post := func(member string, score float64) error {
		status, data, err := call(ctx, http.MethodPost, a.URL()+"/leaderboard/season1", fmt.Sprintf(`{"member":%q,"score":%v}`, member, score))
		if err == nil && status != http.StatusOK {
			err = fmt.Errorf("POST %s: HTTP %d %s", member, status, data)
		}
		return err
	}
	show := func(title string) error {
		_, data, err := call(ctx, http.MethodGet, b.URL()+"/leaderboard/season1/top?n=5", "")
		if err != nil {
			return err
		}
		var body struct {
			Entries []struct {
				Member string  `json:"member"`
				Score  float64 `json:"score"`
			} `json:"entries"`
		}
		if err := json.Unmarshal(data, &body); err != nil {
			return err
		}
		parts := make([]string, 0, len(body.Entries))
		for i, e := range body.Entries {
			parts = append(parts, fmt.Sprintf("%d. %s=%v", i+1, e.Member, e.Score))
		}
		fmt.Printf("%s: %s\n", title, strings.Join(parts, "  "))
		return nil
	}
	for _, p := range []struct {
		member string
		score  float64
	}{{"alice", 120}, {"bob", 300}, {"carol", 200}, {"dave", 200}} {
		if err := post(p.member, p.score); err != nil {
			return err
		}
	}
	if err := show("Top 5 sau 4 lần POST"); err != nil {
		return err
	}
	fmt.Println("carol và dave cùng 200 điểm: Redis xếp member theo thứ tự từ điển, ZRANGE REV đảo chiều nên dave đứng trước.")
	if err := post("alice", 350); err != nil {
		return err
	}
	if err := show("alice được đặt lại 350 điểm (ZADD đặt score, không cộng dồn)"); err != nil {
		return err
	}
	status, data, err := call(ctx, http.MethodGet, b.URL()+"/leaderboard/season1/top?n=0", "")
	if err != nil {
		return err
	}
	fmt.Printf("GET top?n=0 trả HTTP %d: %s\n", status, strings.TrimSpace(string(data)))
	return nil
}

func disconnectDemo(ctx context.Context, rdb *redis.Client, prefix string, a *lab.Server, peers *[]*peer) error {
	fmt.Println("== 3. Client đứt đột ngột: instance dọn phòng và unsubscribe ==")
	channel := lab.RoomChannel(prefix, "lonely")
	dave, err := join(ctx, a, "dave", "lonely")
	if err != nil {
		return err
	}
	*peers = append(*peers, dave)
	if err := until("instance A subscribe phòng lonely", func() bool { return numSub(ctx, rdb, channel) == 1 }); err != nil {
		return err
	}
	fmt.Printf("Instance A có %d client trong phòng lonely, NUMSUB = %d.\n", a.ClientCount("lonely"), numSub(ctx, rdb, channel))
	_ = dave.conn.CloseNow()
	if err := until("instance A bỏ client đã đứt", func() bool { return a.ClientCount("lonely") == 0 }); err != nil {
		return err
	}
	if err := until("instance A unsubscribe channel", func() bool { return numSub(ctx, rdb, channel) == 0 }); err != nil {
		return err
	}
	fmt.Printf("Sau CloseNow (không có close frame): clientCount = %d, NUMSUB = %d. Instance đã unsubscribe vì không còn client nào.\n", a.ClientCount("lonely"), numSub(ctx, rdb, channel))
	fmt.Println()
	return nil
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		return err
	}
	rdb := redis.NewClient(opts)
	prefix := testkit.UniqueName("demo-uc04")

	var (
		errs    []error
		servers []*lab.Server
		peers   []*peer
	)
	demo := func() error {
		a, err := lab.NewServer(ctx, lab.Options{RedisURL: redisURL(), Prefix: prefix})
		if err != nil {
			return err
		}
		servers = append(servers, a)
		b, err := lab.NewServer(ctx, lab.Options{RedisURL: redisURL(), Prefix: prefix})
		if err != nil {
			return err
		}
		servers = append(servers, b)
		if err := chatDemo(ctx, rdb, prefix, a, b, &peers); err != nil {
			return err
		}
		if err := leaderboardDemo(ctx, a, b); err != nil {
			return err
		}
		fmt.Println()
		return disconnectDemo(ctx, rdb, prefix, a, &peers)
	}
	if err := demo(); err != nil {
		errs = append(errs, err)
	}

	// Dọn theo pha tuần tự: đóng client, shutdown instance, xóa key, kiểm tra subscription, đóng rdb.
	for _, p := range peers {
		_ = p.conn.CloseNow()
		<-p.done
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	for _, s := range servers {
		if err := s.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := deleteByPrefix(shutdownCtx, rdb, prefix); err != nil {
		errs = append(errs, err)
	}
	if channels, err := rdb.PubSubChannels(shutdownCtx, escapeGlob(prefix)+"*").Result(); err != nil || len(channels) > 0 {
		errs = append(errs, fmt.Errorf("channel còn sót: %v (lỗi %v)", channels, err))
	}
	if err := rdb.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	fmt.Println("Đã đóng mọi client và instance, xóa mọi key và không còn subscription nào của demo.")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Demo lỗi:", err)
		os.Exit(1)
	}
}
