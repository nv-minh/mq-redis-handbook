package lab

import (
	"bytes"
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
	"testing"
	"time"

	"github.com/coder/websocket"
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

func scanKeys(ctx context.Context, rdb *redis.Client, prefix string) ([]string, error) {
	var (
		found  []string
		cursor uint64
	)
	for {
		keys, next, err := rdb.Scan(ctx, cursor, escapeGlob(prefix)+"*", 200).Result()
		if err != nil {
			return nil, err
		}
		found = append(found, keys...)
		cursor = next
		if cursor == 0 {
			return found, nil
		}
	}
}

// waitUntil poll điều kiện với timeout ngắn hơn timeout của test, và message nói rõ đang chờ gì.
// Chạy trên goroutine của test (Fatalf), không dùng trong goroutine khác.
func waitUntil(t *testing.T, what string, fn func() bool) {
	t.Helper()
	const timeout = 5 * time.Second
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !fn() {
		select {
		case <-deadline.C:
			t.Fatalf("hết %s khi chờ: %s", timeout, what)
		case <-ticker.C:
		}
	}
}

// closeBody đóng body của response mà websocket.Dial trả về (thư viện không bắt buộc, linter bodyclose thì có).
func closeBody(response *http.Response) {
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
}

// testClient là một WebSocket client với một goroutine đọc gom message. Goroutine đó chỉ dùng t.Errorf.
type testClient struct {
	conn *websocket.Conn
	done chan struct{}

	mu       sync.Mutex
	received []ChatMessage
	closeErr error
	closed   bool
}

func (c *testClient) messages() []ChatMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ChatMessage(nil), c.received...)
}

func (c *testClient) texts() []string {
	var out []string
	for _, m := range c.messages() {
		out = append(out, m.Text)
	}
	return out
}

// closeStatus trả (close code, đã đóng chưa). Code là -1 khi socket đứt mà không có close frame.
func (c *testClient) closeStatus() (websocket.StatusCode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		return 0, false
	}
	return websocket.CloseStatus(c.closeErr), true
}

func (c *testClient) closeReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var closeErr websocket.CloseError
	if errors.As(c.closeErr, &closeErr) {
		return closeErr.Reason
	}
	return ""
}

func (c *testClient) send(ctx context.Context, text string) error {
	payload, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return err
	}
	return c.conn.Write(ctx, websocket.MessageText, payload)
}

// env gom client Redis của test, prefix duy nhất và mọi tài nguyên cần dọn.
type env struct {
	t       *testing.T
	rdb     *redis.Client
	prefix  string
	mu      sync.Mutex
	servers []*Server
	clients []*testClient
	stuck   []*websocket.Conn
	cancel  context.CancelFunc
	ctx     context.Context
}

// newEnv đăng ký MỘT hàm dọn chạy các pha tuần tự, lỗi của pha nào cũng được báo mà không chặn pha sau:
// đóng client, shutdown instance, xóa key bằng SCAN, kiểm tra không sót key hay subscription, đóng rdb.
func newEnv(t *testing.T, label string) *env {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &env{t: t, rdb: redis.NewClient(opts), prefix: testkit.UniqueName("uc04-" + label), ctx: ctx, cancel: cancel}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		// Pha 1: đóng mọi client (CloseNow không cần handshake) và chờ goroutine đọc thoát.
		cancel()
		e.mu.Lock()
		clients, stuck, servers := e.clients, e.stuck, e.servers
		e.mu.Unlock()
		for _, conn := range stuck {
			_ = conn.CloseNow()
		}
		for _, c := range clients {
			_ = c.conn.CloseNow()
		}
		for _, c := range clients {
			select {
			case <-c.done:
			case <-cleanupCtx.Done():
				t.Errorf("goroutine đọc của client chưa thoát")
			}
		}
		// Pha 2: shutdown mọi instance. Shutdown trả nil nghĩa là mọi goroutine của instance đã thoát.
		var wg sync.WaitGroup
		for _, s := range servers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.Shutdown(cleanupCtx); err != nil {
					t.Errorf("shutdown instance: %v", err)
				}
			}()
		}
		wg.Wait()
		// Pha 3: xóa key của test bằng SCAN theo prefix (không bao giờ KEYS trên Redis dùng chung).
		keys, err := scanKeys(cleanupCtx, e.rdb, e.prefix)
		if err != nil {
			t.Errorf("SCAN dọn dẹp: %v", err)
		} else if len(keys) > 0 {
			if err := e.rdb.Del(cleanupCtx, keys...).Err(); err != nil {
				t.Errorf("DEL dọn dẹp: %v", err)
			}
		}
		// Pha 4: không còn key hay subscription nào của test sót lại.
		left, err := scanKeys(cleanupCtx, e.rdb, e.prefix)
		if err != nil || len(left) > 0 {
			t.Errorf("key còn sót của prefix %s: %v (lỗi %v)", e.prefix, left, err)
		}
		// Redis dọn subscription của một connection đã đóng một cách bất đồng bộ (khi nó đọc tới EOF), nên chờ
		// có giới hạn thay vì khẳng định ngay.
		var channels []string
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			channels, err = e.rdb.PubSubChannels(cleanupCtx, escapeGlob(e.prefix)+"*").Result()
			if err != nil || len(channels) == 0 {
				break
			}
			select {
			case <-ticker.C:
				continue
			case <-cleanupCtx.Done():
			}
			break
		}
		if err != nil || len(channels) > 0 {
			t.Errorf("channel còn sót của prefix %s: %v (lỗi %v)", e.prefix, channels, err)
		}
		if err := e.rdb.Close(); err != nil {
			t.Errorf("đóng client redis: %v", err)
		}
	})
	return e
}

func (e *env) startServer(overrides func(*Options)) *Server {
	e.t.Helper()
	opts := Options{
		RedisURL: redisURL(),
		Prefix:   e.prefix,
		OnError:  func(err error) { e.t.Logf("OnError: %v", err) },
	}
	if overrides != nil {
		overrides(&opts)
	}
	s, err := NewServer(e.ctx, opts)
	if err != nil {
		e.t.Fatalf("NewServer: %v", err)
	}
	e.mu.Lock()
	e.servers = append(e.servers, s)
	e.mu.Unlock()
	return s
}

func (e *env) channel(room string) string { return RoomChannel(e.prefix, room) }

func (e *env) numSub(channel string) int64 {
	counts, err := e.rdb.PubSubNumSub(e.ctx, channel).Result()
	if err != nil {
		e.t.Fatalf("PUBSUB NUMSUB: %v", err)
	}
	return counts[channel]
}

// dial mở một client và gom message. Mọi client đều được đóng trong cleanup.
func (e *env) dial(s *Server, room, name string, header http.Header) *testClient {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, s.WSURL(room, name), &websocket.DialOptions{HTTPHeader: header})
	closeBody(response)
	if err != nil {
		e.t.Fatalf("Dial %s/%s: %v", room, name, err)
	}
	// Message của slow-client test lớn hơn mặc định 32 KiB của thư viện khi tính cả phong bì JSON.
	conn.SetReadLimit(1 << 20)
	c := &testClient{conn: conn, done: make(chan struct{})}
	e.mu.Lock()
	e.clients = append(e.clients, c)
	e.mu.Unlock()
	go func() {
		defer close(c.done)
		for {
			typ, data, err := conn.Read(e.ctx)
			if err != nil {
				c.mu.Lock()
				c.closeErr, c.closed = err, true
				c.mu.Unlock()
				return
			}
			var msg ChatMessage
			if typ != websocket.MessageText || json.Unmarshal(data, &msg) != nil {
				t := e.t
				t.Errorf("client nhận frame không phải JSON ChatMessage: %q", data)
				continue
			}
			c.mu.Lock()
			c.received = append(c.received, msg)
			c.mu.Unlock()
		}
	}()
	return c
}

// dialStuck mở một client mà KHÔNG BAO GIỜ đọc: client của coder/websocket chỉ đọc khi ta gọi Read,
// nên socket này không nhận dữ liệu và không trả lời ping.
func (e *env) dialStuck(s *Server, room string) *websocket.Conn {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, s.WSURL(room, "stuck"), nil)
	closeBody(response)
	if err != nil {
		e.t.Fatalf("Dial stuck: %v", err)
	}
	conn.SetReadLimit(1 << 20)
	e.mu.Lock()
	e.stuck = append(e.stuck, conn)
	e.mu.Unlock()
	return conn
}

func (e *env) waitMessages(c *testClient, count int, what string) {
	e.t.Helper()
	waitUntil(e.t, what, func() bool { return len(c.messages()) >= count })
}

func (e *env) waitClosed(c *testClient, what string) (websocket.StatusCode, string) {
	e.t.Helper()
	waitUntil(e.t, what, func() bool { _, closed := c.closeStatus(); return closed })
	code, _ := c.closeStatus()
	return code, c.closeReason()
}

func (e *env) sendText(c *testClient, text string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	if err := c.send(ctx, text); err != nil {
		e.t.Fatalf("gửi %q: %v", text, err)
	}
}

// httpDo gọi HTTP và trả status cùng body đã đọc hết, luôn đóng body.
func httpDo(t *testing.T, method, url string, body string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("đọc body: %v", err)
	}
	return response.StatusCode, data
}

type entry struct {
	Member string
	Score  float64
}

// topEntries gọi GET top và trả status cùng các entry. Khẳng định luôn rằng score là number trong JSON.
func topEntries(t *testing.T, s *Server, name, query string) (int, []entry, string) {
	t.Helper()
	status, data := httpDo(t, http.MethodGet, s.URL()+"/leaderboard/"+name+"/top"+query, "")
	var body struct {
		Name    string
		Entries []map[string]any
		Error   string
	}
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("body không phải JSON: %q", data)
	}
	var entries []entry
	for _, raw := range body.Entries {
		score, isNumber := raw["score"].(float64)
		if !isNumber {
			t.Fatalf("score phải là number trong JSON, nhận %T (%v)", raw["score"], raw["score"])
		}
		entries = append(entries, entry{Member: raw["member"].(string), Score: score})
	}
	if status == http.StatusOK && body.Entries == nil {
		t.Fatalf("entries phải là mảng (có thể rỗng), không phải null: %s", data)
	}
	return status, entries, body.Error
}

func postScore(t *testing.T, s *Server, name, body string) int {
	t.Helper()
	status, _ := httpDo(t, http.MethodPost, s.URL()+"/leaderboard/"+name, body)
	return status
}

func TestClientOnInstanceAReceivesMessageSentViaInstanceB(t *testing.T) {
	e := newEnv(t, "fanout")
	instanceA, instanceB := e.startServer(nil), e.startServer(nil)
	room, otherRoom := "general", "random"

	alice := e.dial(instanceA, room, "alice", nil)
	bob := e.dial(instanceB, room, "bob", nil)
	// Client của phòng khác nằm trên instance B, tức dùng CHUNG subscriber connection với bob.
	// Nhờ vậy thứ tự đến của hai channel trên connection đó chứng minh được việc không nhận nhầm.
	carol := e.dial(instanceB, otherRoom, "carol", nil)

	// Sẵn sàng một cách xác định: cả hai instance đã subscribe channel của phòng (PUBSUB NUMSUB = 2).
	waitUntil(t, "cả hai instance subscribe channel của phòng", func() bool { return e.numSub(e.channel(room)) == 2 })
	if instanceA.ClientCount(room) != 1 || instanceB.ClientCount(room) != 1 {
		t.Fatalf("mỗi instance phải có đúng 1 client: A=%d B=%d", instanceA.ClientCount(room), instanceB.ClientCount(room))
	}

	before := time.Now().UnixMilli()
	e.sendText(bob, "hello from B")
	e.waitMessages(alice, 1, "alice nhận message đầu tiên")
	e.waitMessages(bob, 1, "bob nhận lại message của chính mình")
	after := time.Now().UnixMilli()

	got := alice.messages()[0]
	if got.Room != room || got.From != "bob" || got.Text != "hello from B" || got.TS < before || got.TS > after {
		t.Fatalf("message của alice sai: %+v (cửa sổ ts %d..%d)", got, before, after)
	}
	// Người gửi cũng nhận message của mình đúng một lần (quyết định: mọi thứ đi qua Redis).
	if bob.messages()[0] != got {
		t.Fatalf("bob nhận %+v, alice nhận %+v: phải giống nhau", bob.messages()[0], got)
	}

	// Phòng khác gửi message SAU. Khi carol nhận được nó, message trước đã được xử lý xong trên cùng
	// subscriber connection, nên nếu nó bị giao nhầm thì đã nằm trong carol.received rồi.
	e.sendText(carol, "only for random")
	e.waitMessages(carol, 1, "carol nhận message của phòng random")
	if texts := carol.texts(); len(texts) != 1 || texts[0] != "only for random" || carol.messages()[0].Room != otherRoom {
		t.Fatalf("carol chỉ được nhận message của phòng random, nhận %v", texts)
	}

	// Marker cuối: alice và bob nhận [message 1, marker], không có bản sao thừa chen giữa
	// (một lần giao trùng sẽ đến trước marker vì cùng đi trên một pipeline).
	e.sendText(bob, "marker")
	e.waitMessages(alice, 2, "alice nhận marker")
	e.waitMessages(bob, 2, "bob nhận marker")
	want := []string{"hello from B", "marker"}
	for name, c := range map[string]*testClient{"alice": alice, "bob": bob} {
		if texts := c.texts(); strings.Join(texts, "|") != strings.Join(want, "|") {
			t.Fatalf("%s nhận %v, muốn %v", name, texts, want)
		}
	}
}

func TestLeaderboardTopReturnsScoresInDescendingOrder(t *testing.T) {
	e := newEnv(t, "lb")
	instanceA, instanceB := e.startServer(nil), e.startServer(nil)

	// Bảng rỗng trả danh sách rỗng, không phải lỗi.
	status, entries, _ := topEntries(t, instanceB, "season1", "?n=5")
	if status != http.StatusOK || len(entries) != 0 {
		t.Fatalf("bảng rỗng: status %d entries %v", status, entries)
	}

	// POST lên instance A, GET từ instance B: dữ liệu nằm ở Redis dùng chung.
	for _, p := range []struct {
		member string
		score  float64
	}{{"alice", 10}, {"bob", 30}, {"carol", 20}, {"dave", 20}, {"eve", 2.5}} {
		body := fmt.Sprintf(`{"member":%q,"score":%v}`, p.member, p.score)
		status, data := httpDo(t, http.MethodPost, instanceA.URL()+"/leaderboard/season1", body)
		if status != http.StatusOK {
			t.Fatalf("POST %s: status %d %s", body, status, data)
		}
		var reply map[string]any
		if err := json.Unmarshal(data, &reply); err != nil || reply["member"] != p.member || reply["score"] != p.score || reply["name"] != "season1" {
			t.Fatalf("POST %s trả %s", body, data)
		}
	}

	expectOrder := func(label string, got []entry, want []entry) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: %v, muốn %v", label, got, want)
		}
	}
	// Giảm dần theo score. Hai member cùng score 20 theo thứ tự GIẢM dần của member (dave trước carol):
	// Redis sắp xếp score bằng nhau theo thứ tự từ điển của member, và ZRANGE ... REV đảo cả hai chiều.
	_, all, _ := topEntries(t, instanceB, "season1", "?n=100")
	expectOrder("top 100", all, []entry{{"bob", 30}, {"dave", 20}, {"carol", 20}, {"alice", 10}, {"eve", 2.5}})

	// n nhỏ hơn kích thước cắt đúng; n lớn hơn kích thước trả đủ; không có n thì mặc định 10.
	_, two, _ := topEntries(t, instanceB, "season1", "?n=2")
	expectOrder("top 2", two, []entry{{"bob", 30}, {"dave", 20}})
	for _, query := range []string{"?n=1000", ""} {
		if _, got, _ := topEntries(t, instanceB, "season1", query); len(got) != 5 {
			t.Fatalf("query %q: muốn 5 entry, nhận %d", query, len(got))
		}
	}

	// n không hợp lệ: 400 kèm message, không bao giờ 500.
	for _, query := range []string{"?n=0", "?n=-3", "?n=abc", "?n=1.5", "?n=", "?n=1001", "?n=1e2"} {
		status, _, message := topEntries(t, instanceB, "season1", query)
		if status != http.StatusBadRequest || message == "" {
			t.Fatalf("GET top %s: status %d, error %q (muốn 400 kèm message)", query, status, message)
		}
	}

	// POST là "đặt score" (ZADD), không phải cộng dồn: cập nhật alice từ 10 lên 40 đưa nó lên đầu,
	// và POST lần nữa với 15 đặt lại đúng 15 (nếu là ZINCRBY thì sẽ ra 55).
	if status := postScore(t, instanceB, "season1", `{"member":"alice","score":40}`); status != http.StatusOK {
		t.Fatalf("POST cập nhật alice: %d", status)
	}
	_, firstTwo, _ := topEntries(t, instanceA, "season1", "?n=2")
	expectOrder("sau khi alice lên 40", firstTwo, []entry{{"alice", 40}, {"bob", 30}})
	postScore(t, instanceA, "season1", `{"member":"alice","score":15}`)
	_, afterReset, _ := topEntries(t, instanceB, "season1", "?n=100")
	expectOrder("sau khi đặt lại alice 15", afterReset, []entry{{"bob", 30}, {"dave", 20}, {"carol", 20}, {"alice", 15}, {"eve", 2.5}})

	// Các bảng độc lập với nhau.
	if _, other, _ := topEntries(t, instanceB, "season2", "?n=5"); len(other) != 0 {
		t.Fatalf("season2 phải rỗng, nhận %v", other)
	}

	// Body hoặc tên sai: 400, và không ghi gì vào Redis.
	for _, body := range []string{
		`not json`, `[]`, `null`, `{"score":1}`, `{"member":"","score":1}`, `{"member":7,"score":1}`,
		`{"member":"x","score":"5"}`, `{"member":"x"}`, `{"member":"x","score":1e999}`,
		fmt.Sprintf(`{"member":%q,"score":1}`, strings.Repeat("m", 129)),
	} {
		if status := postScore(t, instanceA, "season2", body); status != http.StatusBadRequest {
			t.Fatalf("POST %s: status %d, muốn 400", body, status)
		}
	}
	if status := postScore(t, instanceA, "bad%20name", `{"member":"x","score":1}`); status != http.StatusBadRequest {
		t.Fatalf("POST tên sai: status %d, muốn 400", status)
	}
	if status, _, _ := topEntries(t, instanceB, "bad%20name", "?n=5"); status != http.StatusBadRequest {
		t.Fatalf("GET tên sai: status %d, muốn 400", status)
	}
	exists, err := e.rdb.Exists(e.ctx, LeaderboardKey(e.prefix, "season2")).Result()
	if err != nil || exists != 0 {
		t.Fatalf("season2 không được tồn tại: exists=%d err=%v", exists, err)
	}
}

func TestDisconnectedClientIsRemovedFromRoom(t *testing.T) {
	e := newEnv(t, "leave")
	instanceA, instanceB := e.startServer(nil), e.startServer(nil)
	room := "lobby"
	channel := e.channel(room)

	if instanceA.RoomCount() != 0 {
		t.Fatalf("instance mới phải chưa có phòng nào: %d", instanceA.RoomCount())
	}
	graceful := e.dial(instanceA, room, "graceful", nil)
	abrupt := e.dial(instanceA, room, "abrupt", nil)
	stayer := e.dial(instanceB, room, "stayer", nil)
	waitUntil(t, "hai instance subscribe channel", func() bool { return e.numSub(channel) == 2 })
	if instanceA.ClientCount(room) != 2 || instanceB.ClientCount(room) != 1 {
		t.Fatalf("A=%d B=%d, muốn 2 và 1", instanceA.ClientCount(room), instanceB.ClientCount(room))
	}

	// Đóng lịch sự: có close handshake. Instance A bỏ client khỏi phòng nhưng còn abrupt nên vẫn subscribe.
	if err := graceful.conn.Close(websocket.StatusNormalClosure, "bye"); err != nil {
		t.Fatalf("đóng lịch sự: %v", err)
	}
	waitUntil(t, "instance A bỏ client đóng lịch sự", func() bool { return instanceA.ClientCount(room) == 1 })
	if n := e.numSub(channel); n != 2 {
		t.Fatalf("A còn client nên vẫn subscribe: NUMSUB %d, muốn 2", n)
	}

	// Message tới phòng trong lúc có client đã rời: người còn lại nhận đúng một lần, không lỗi.
	e.sendText(stayer, "after graceful leave")
	e.waitMessages(abrupt, 1, "client abrupt nhận message")
	e.waitMessages(stayer, 1, "stayer nhận message của chính mình")
	if got := graceful.messages(); len(got) != 0 {
		t.Fatalf("client đã đóng không được nhận gì: %v", got)
	}

	// Đứt đột ngột: CloseNow đóng TCP connection mà không gửi close frame. Instance phải tự phát hiện.
	if err := abrupt.conn.CloseNow(); err != nil {
		t.Fatalf("CloseNow: %v", err)
	}
	waitUntil(t, "instance A phát hiện socket bị hủy và bỏ client", func() bool { return instanceA.ClientCount(room) == 0 })
	// Client cuối của instance A đã rời: nó unsubscribe, chỉ còn instance B.
	waitUntil(t, "instance A unsubscribe channel (NUMSUB về 1)", func() bool { return e.numSub(channel) == 1 })
	if instanceA.RoomCount() != 0 {
		t.Fatalf("instance A không còn phòng nào, nhận %d", instanceA.RoomCount())
	}

	// Phòng còn instance B: message vẫn đi qua Redis tới đúng người, instance A không lỗi.
	e.sendText(stayer, "after abrupt leave")
	e.waitMessages(stayer, 2, "stayer nhận message thứ hai")
	if texts := stayer.texts(); strings.Join(texts, "|") != "after graceful leave|after abrupt leave" {
		t.Fatalf("stayer nhận %v", texts)
	}

	// Client cuối của instance B rời: NUMSUB về 0, không còn phòng nào trên cả hai instance.
	if err := stayer.conn.Close(websocket.StatusNormalClosure, "bye"); err != nil {
		t.Fatalf("stayer đóng: %v", err)
	}
	waitUntil(t, "instance B unsubscribe channel (NUMSUB về 0)", func() bool { return e.numSub(channel) == 0 })
	if instanceB.ClientCount(room) != 0 || instanceB.RoomCount() != 0 {
		t.Fatalf("B: clients=%d rooms=%d, muốn 0 và 0", instanceB.ClientCount(room), instanceB.RoomCount())
	}

	// Publish tới phòng không còn ai: 0 receiver, không lỗi, không có gì bị giữ lại.
	if receivers, err := e.rdb.Publish(e.ctx, channel, `{"text":"nobody home"}`).Result(); err != nil || receivers != 0 {
		t.Fatalf("PUBLISH tới phòng trống: receivers=%d err=%v, muốn 0", receivers, err)
	}
	// Phòng có thể dùng lại: client mới subscribe lại và nhận message.
	returning := e.dial(instanceA, room, "returning", nil)
	waitUntil(t, "instance A subscribe lại", func() bool { return e.numSub(channel) == 1 })
	e.sendText(returning, "welcome back")
	e.waitMessages(returning, 1, "client quay lại nhận message")
}

func TestInvalidRoomIsRejectedWith400BeforeTheUpgrade(t *testing.T) {
	e := newEnv(t, "room")
	s := e.startServer(nil)
	base := strings.SplitN(s.WSURL("x", ""), "?", 2)[0]
	for _, c := range []struct{ label, url, field string }{
		{"thiếu room", base, "room"},
		{"room rỗng", base + "?room=", "room"},
		{"room quá dài (65 ký tự)", base + "?room=" + strings.Repeat("r", 65), "room"},
		{"room có ký tự không cho phép", base + "?room=a%20b", "room"},
		{"room có glob", base + "?room=a*", "room"},
		{"name quá dài", base + "?room=ok&name=" + strings.Repeat("n", 65), "name"},
	} {
		ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
		conn, response, err := websocket.Dial(ctx, c.url, nil)
		cancel()
		if err == nil {
			_ = conn.CloseNow()
			t.Fatalf("%s: Dial phải thất bại", c.label)
		}
		if response == nil || response.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: muốn HTTP 400, nhận %v (lỗi %v)", c.label, response, err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if !strings.Contains(string(body), c.field) {
			t.Fatalf("%s: body %q phải nhắc tới %q", c.label, body, c.field)
		}
	}
	// Room dài đúng 64 ký tự thì được.
	ok := e.dial(s, strings.Repeat("r", 64), "", nil)
	if err := ok.conn.Close(websocket.StatusNormalClosure, ""); err != nil {
		t.Fatalf("đóng client room 64 ký tự: %v", err)
	}
	// Đường dẫn khác /ws không phải endpoint WebSocket.
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, strings.Replace(base, "/ws", "/nope", 1)+"?room=ok", nil)
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("Dial /nope phải thất bại")
	}
	if response == nil || response.StatusCode != http.StatusNotFound {
		t.Fatalf("/nope: muốn 404, nhận %v", response)
	}
	_ = response.Body.Close()
	// Chưa phòng nào được tạo bởi các request bị từ chối (phòng 64 ký tự đã được dọn).
	waitUntil(t, "server không còn phòng nào", func() bool { return s.RoomCount() == 0 })
}

func TestCrossOriginUpgradeIsRejectedButSameOriginAndNoOriginAreAllowed(t *testing.T) {
	e := newEnv(t, "origin")
	s := e.startServer(nil)
	// Lab chạy trên localhost: client không gửi Origin (không phải trình duyệt) và origin cùng host được nhận,
	// origin của trang web khác bị từ chối bằng 403.
	e.dial(s, "room-o", "", nil)
	e.dial(s, "room-o", "", http.Header{"Origin": {s.URL()}})
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, s.WSURL("room-o", ""), &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"http://evil.example"}}})
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("origin lạ phải bị từ chối")
	}
	if response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("muốn 403, nhận %v (lỗi %v)", response, err)
	}
	_ = response.Body.Close()
}

func TestMalformedMessageClosesOnlyThatConnectionWith1003(t *testing.T) {
	e := newEnv(t, "malformed")
	s := e.startServer(nil)
	room := "bad-input"
	healthy := e.dial(s, room, "healthy", nil)

	for _, c := range []struct {
		label   string
		typ     websocket.MessageType
		payload []byte
	}{
		{"không phải JSON", websocket.MessageText, []byte("this is not json")},
		{"JSON nhưng không phải object", websocket.MessageText, []byte("[1,2,3]")},
		{"thiếu text", websocket.MessageText, []byte(`{"message":"hi"}`)},
		{"text không phải chuỗi", websocket.MessageText, []byte(`{"text":42}`)},
		{"text rỗng", websocket.MessageText, []byte(`{"text":""}`)},
		{"frame nhị phân", websocket.MessageBinary, []byte{1, 2, 3}},
	} {
		offender := e.dial(s, room, "offender", nil)
		ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
		err := offender.conn.Write(ctx, c.typ, c.payload)
		cancel()
		if err != nil {
			t.Fatalf("%s: ghi: %v", c.label, err)
		}
		code, reason := e.waitClosed(offender, c.label+": server đóng connection")
		if code != websocket.StatusUnsupportedData || !strings.Contains(reason, "JSON") {
			t.Fatalf("%s: close code %d reason %q, muốn 1003 và lý do nhắc tới JSON", c.label, code, reason)
		}
	}

	// Instance vẫn sống và client khỏe vẫn nhận được message, không nhận message lỗi nào.
	e.sendText(healthy, "still alive")
	e.waitMessages(healthy, 1, "client khỏe nhận message")
	if texts := healthy.texts(); len(texts) != 1 || texts[0] != "still alive" {
		t.Fatalf("client khỏe nhận %v", texts)
	}
	waitUntil(t, "chỉ còn client khỏe trong phòng", func() bool { return s.ClientCount(room) == 1 })
}

func TestOversizedMessageClosesConnectionWith1009AndInstanceSurvives(t *testing.T) {
	e := newEnv(t, "big")
	s := e.startServer(func(o *Options) { o.MaxMessageBytes = 1024 })
	room := "size-limit"
	healthy := e.dial(s, room, "healthy", nil)
	sender := e.dial(s, room, "sender", nil)

	// Dưới giới hạn thì qua (cả frame JSON dưới 1024 byte).
	e.sendText(sender, strings.Repeat("x", 900))
	e.waitMessages(healthy, 1, "message dưới giới hạn được giao")

	// Vượt giới hạn: thư viện đóng connection với 1009 (Message Too Big) trước khi gom hết payload.
	e.sendText(sender, strings.Repeat("y", 5000))
	if code, _ := e.waitClosed(sender, "server đóng client gửi message quá lớn"); code != websocket.StatusMessageTooBig {
		t.Fatalf("close code %d, muốn 1009", code)
	}

	e.sendText(healthy, "after the big one")
	e.waitMessages(healthy, 2, "client khỏe nhận message sau đó")
	// Message quá lớn không bao giờ tới ai: chỉ có hai message hợp lệ.
	if texts := healthy.texts(); len(texts) != 2 || texts[0] != strings.Repeat("x", 900) || texts[1] != "after the big one" {
		t.Fatalf("client khỏe nhận %d message: %.60v", len(texts), texts)
	}
}

func TestSlowClientIsDroppedWithoutBlockingOthers(t *testing.T) {
	e := newEnv(t, "slow")
	// Message 30 KB và hàng đợi gửi tối đa 8 message mỗi client.
	s := e.startServer(func(o *Options) { o.MaxMessageBytes = 32768; o.SendQueue = 8 })
	room := "slow-room"

	// Client "kẹt": hoàn tất handshake rồi KHÔNG BAO GIỜ đọc nữa.
	stuck := e.dialStuck(s, room)
	fast := e.dial(s, room, "fast", nil)
	waitUntil(t, "instance subscribe channel", func() bool { return e.numSub(e.channel(room)) == 1 })
	if s.ClientCount(room) != 2 {
		t.Fatalf("muốn 2 client, nhận %d", s.ClientCount(room))
	}

	// Gửi từng đợt nhỏ và chờ client nhanh nhận hết đợt đó trước khi gửi tiếp, nên hàng đợi của client nhanh
	// không bao giờ vượt một đợt. Client kẹt không đọc, nên socket của nó đầy rồi hàng đợi của nó đầy.
	// Vòng lặp dừng khi server bỏ nó (có chặn trên để test không treo vô hạn).
	text := strings.Repeat("z", 30_000)
	const burst = 2
	sent := 0
	for round := 0; s.ClientCount(room) == 2; round++ {
		if round >= 600 {
			t.Fatal("server không bỏ client kẹt sau 600 đợt (36 MB)")
		}
		for i := 0; i < burst; i++ {
			e.sendText(fast, text)
		}
		sent += burst
		e.waitMessages(fast, sent, fmt.Sprintf("client nhanh nhận đủ %d message", sent))
	}
	if n := s.ClientCount(room); n != 1 {
		t.Fatalf("muốn còn 1 client, nhận %d", n)
	}

	// Server đã hủy TCP connection của client kẹt: đọc nốt dữ liệu còn trong buffer rồi phải gặp lỗi.
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	for {
		if _, _, err := stuck.Read(ctx); err != nil {
			if ctx.Err() != nil {
				t.Fatalf("socket của client kẹt không bị server đóng: %v", err)
			}
			break
		}
	}

	// Client nhanh nhận đủ, vẫn kết nối, và vẫn gửi nhận được sau khi client kẹt bị bỏ.
	if got := len(fast.messages()); got != sent {
		t.Fatalf("client nhanh nhận %d message, muốn %d", got, sent)
	}
	e.sendText(fast, "after drop")
	e.waitMessages(fast, sent+1, "client nhanh nhận message sau khi bỏ client kẹt")
	if texts := fast.texts(); texts[len(texts)-1] != "after drop" {
		t.Fatalf("message cuối là %q", texts[len(texts)-1])
	}
}

func TestUnresponsiveClientIsDroppedByHeartbeat(t *testing.T) {
	e := newEnv(t, "hb")
	s := e.startServer(func(o *Options) { o.PingInterval = 50 * time.Millisecond })
	room := "heartbeat"
	// Client kẹt không bao giờ trả pong (không gọi Read). Client khỏe (đang Read) tự trả pong nên ở lại.
	e.dialStuck(s, room)
	healthy := e.dial(s, room, "healthy", nil)
	if s.ClientCount(room) != 2 {
		t.Fatalf("muốn 2 client, nhận %d", s.ClientCount(room))
	}
	waitUntil(t, "heartbeat bỏ client không trả pong", func() bool { return s.ClientCount(room) == 1 })
	e.sendText(healthy, "still here")
	e.waitMessages(healthy, 1, "client khỏe vẫn nhận message")
}

func TestShutdownClosesEverythingAndIsIdempotent(t *testing.T) {
	e := newEnv(t, "shutdown")
	s := e.startServer(nil)
	room := "closing"
	one := e.dial(s, room, "one", nil)
	two := e.dial(s, "closing-2", "two", nil)
	waitUntil(t, "instance subscribe hai channel", func() bool { return e.numSub(e.channel(room)) == 1 })

	// Gọi đồng thời hai lần và gọi lại sau khi xong: mọi lần cùng trả nil.
	var wg sync.WaitGroup
	results := make([]error, 3)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			results[i] = s.Shutdown(ctx)
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results[2] = s.Shutdown(ctx)
	for i, err := range results {
		if err != nil {
			t.Fatalf("lần Shutdown thứ %d lỗi: %v", i+1, err)
		}
	}

	for name, c := range map[string]*testClient{"one": one, "two": two} {
		if code, _ := e.waitClosed(c, "client "+name+" nhận close frame"); code != websocket.StatusGoingAway {
			t.Fatalf("client %s: close code %d, muốn 1001", name, code)
		}
	}
	if s.ClientCount(room) != 0 || s.RoomCount() != 0 {
		t.Fatalf("clients=%d rooms=%d, muốn 0 và 0", s.ClientCount(room), s.RoomCount())
	}
	// Cả hai connection Redis của instance đã đóng.
	if err := s.rdb.Ping(ctx).Err(); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("connection lệnh thường phải đã đóng, Ping trả %v", err)
	}
	if err := s.pubsub.Ping(ctx); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("subscriber phải đã đóng, Ping trả %v", err)
	}
	// Redis dọn subscription của connection đã đóng một cách bất đồng bộ (khi nó đọc tới EOF), nên dùng waitUntil.
	waitUntil(t, "NUMSUB về 0 sau shutdown", func() bool { return e.numSub(e.channel(room)) == 0 })
	// HTTP server đã dừng: không nhận kết nối mới.
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.URL()+"/leaderboard/x/top?n=1", bytes.NewReader(nil))
	if response, err := http.DefaultClient.Do(request); err == nil {
		_ = response.Body.Close()
		t.Fatal("server đã shutdown vẫn nhận request")
	}
	conn, response, err := websocket.Dial(ctx, s.WSURL(room, ""), nil)
	closeBody(response)
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("server đã shutdown vẫn nhận WebSocket")
	}
}
