// Package lab dựng một instance server realtime: WebSocket fan-out qua Redis Pub/Sub và bảng xếp hạng
// trên sorted set. Chạy nhiều instance trong cùng process (mỗi instance một cổng) để mô phỏng nhiều máy
// dùng chung một Redis.
package lab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
)

// ChatMessage là message client nhận được. Server điền From và TS, client chỉ gửi {"text": "..."}.
type ChatMessage struct {
	Room string `json:"room"`
	From string `json:"from"`
	Text string `json:"text"`
	TS   int64  `json:"ts"`
}

// Options cấu hình một instance. Giá trị 0 nghĩa là dùng mặc định ghi bên cạnh.
type Options struct {
	RedisURL string
	// Prefix của mọi channel và key (tách theo môi trường, và để test dùng UniqueName).
	Prefix string
	// MaxMessageBytes là giới hạn kích thước MỘT message client gửi lên (byte của frame). Mặc định 4096.
	// coder/websocket mặc định 32768 (SetReadLimit), nên đặt tường minh để giới hạn không phụ thuộc thư viện.
	MaxMessageBytes int64
	// SendQueue là số message tối đa đang chờ gửi cho một client. Đầy thì client bị hủy (client chậm). Mặc định 16.
	SendQueue int
	// PingInterval là chu kỳ ping heartbeat. Client không trả pong trong một chu kỳ thì bị hủy. Mặc định 30 s.
	PingInterval time.Duration
	// WriteTimeout giới hạn một lần ghi xuống socket của client. Mặc định 5 s.
	WriteTimeout time.Duration
	// CloseGrace là thời gian chờ close handshake khi shutdown trước khi hủy hẳn. Mặc định 2 s.
	CloseGrace time.Duration
	// OnError nhận các lỗi không có chỗ nào khác để báo (Redis, socket). Mặc định bỏ qua.
	OnError func(error)
}

const (
	nameRule        = "gồm 1 đến 64 ký tự trong [A-Za-z0-9._-]"
	maxMemberLength = 128
	maxTop          = 1000
	defaultTop      = 10
	maxBodyBytes    = 1024
	// closeBadMessage là lý do kèm close code 1003, tối đa 123 byte theo RFC 6455.
	closeBadMessage = `chỉ nhận text JSON dạng {"text": "..."} không rỗng`
)

// namePattern áp cho tên room, tên hiển thị và tên bảng xếp hạng: an toàn để ghép vào tên channel và key.
var (
	namePattern  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	leaderRoute  = regexp.MustCompile(`^/leaderboard/([^/]+)(/top)?$`)
	digitsOnly   = regexp.MustCompile(`^[0-9]+$`)
	errClosing   = errors.New("server đang tắt")
	errBadOrigin = errors.New("origin không được phép")
)

// RoomChannel là channel Pub/Sub của một phòng.
func RoomChannel(prefix, room string) string { return prefix + ":room:" + room }

// LeaderboardKey là key sorted set của một bảng xếp hạng.
func LeaderboardKey(prefix, name string) string { return prefix + ":lb:" + name }

type room struct {
	name    string
	channel string
	clients map[*client]struct{}
	// refs là số client trong phòng cộng các lần upgrade đang chờ subscribe xong. Về 0 thì unsubscribe.
	refs int
	// ready đóng khi Redis xác nhận SUBSCRIBE: từ đó mọi PUBLISH tới channel đều tới instance này.
	ready chan struct{}
}

type client struct {
	name string
	room *room
	conn *websocket.Conn
	// send là hàng đợi gửi có giới hạn. Đầy nghĩa là client chậm.
	send   chan []byte
	ctx    context.Context
	cancel context.CancelFunc
}

// Server là một instance realtime.
type Server struct {
	opts   Options
	rdb    *redis.Client
	pubsub *redis.PubSub
	http   *http.Server
	url    string
	port   int

	baseCtx    context.Context
	baseCancel context.CancelFunc

	mu      sync.Mutex
	rooms   map[string]*room
	waiters map[string][]chan struct{}
	clients map[*client]struct{}
	closing bool
	// wg đếm mọi goroutine của instance: vòng Serve, vòng nhận từ Redis và mỗi handler WebSocket
	// (cùng goroutine ghi và heartbeat của nó). Shutdown chờ wg về 0 để chứng minh không rò goroutine.
	wg sync.WaitGroup

	shutdownOnce sync.Once
	shutdownErr  error
}

// NewServer mở hai kết nối Redis (lệnh thường và subscriber riêng) và một HTTP server trên cổng 0.
// ctx chỉ dùng cho việc khởi động; vòng đời của server do Shutdown quyết định.
func NewServer(ctx context.Context, opts Options) (*Server, error) {
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = 4096
	}
	if opts.SendQueue <= 0 {
		opts.SendQueue = 16
	}
	if opts.PingInterval <= 0 {
		opts.PingInterval = 30 * time.Second
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = 5 * time.Second
	}
	if opts.CloseGrace <= 0 {
		opts.CloseGrace = 2 * time.Second
	}
	if opts.OnError == nil {
		opts.OnError = func(error) {}
	}
	redisOpts, err := redis.ParseURL(opts.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("redis url không hợp lệ: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = rdb.Close()
		return nil, err
	}

	baseCtx, baseCancel := context.WithCancel(context.Background())
	s := &Server{
		opts:       opts,
		rdb:        rdb,
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
		rooms:      map[string]*room{},
		waiters:    map[string][]chan struct{}{},
		clients:    map[*client]struct{}{},
	}
	// Subscriber dành riêng: PubSub của go-redis có connection riêng, không dùng chung socket với rdb.
	s.pubsub = rdb.Subscribe(ctx)
	s.port = listener.Addr().(*net.TCPAddr).Port
	s.url = "http://" + listener.Addr().String()
	s.http = &http.Server{Handler: http.HandlerFunc(s.route), ReadHeaderTimeout: 10 * time.Second}

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		if err := s.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			opts.OnError(fmt.Errorf("http serve: %w", err))
		}
	}()
	go s.receiveLoop()
	return s, nil
}

// URL là URL HTTP gốc, ví dụ http://127.0.0.1:41234.
func (s *Server) URL() string { return s.url }

// WSURL là URL WebSocket của một phòng, kèm tên hiển thị tùy chọn (chuỗi rỗng là để server tự đặt).
func (s *Server) WSURL(room, name string) string {
	u := fmt.Sprintf("ws://127.0.0.1:%d/ws?room=%s", s.port, url.QueryEscape(room))
	if name != "" {
		u += "&name=" + url.QueryEscape(name)
	}
	return u
}

// ClientCount là số client cục bộ (trên instance này) đang ở trong phòng.
func (s *Server) ClientCount(room string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rm := s.rooms[room]; rm != nil {
		return len(rm.clients)
	}
	return 0
}

// RoomCount là số phòng mà instance này đang subscribe (phòng có ít nhất một client cục bộ).
func (s *Server) RoomCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rooms)
}

// ---- Phòng và vòng đời subscribe ----

// acquireRoom giữ chỗ trong phòng và chờ Redis xác nhận subscribe. Lần đầu tiên của một phòng gửi SUBSCRIBE
// ngay trong lúc giữ mutex, nên thứ tự SUBSCRIBE và UNSUBSCRIBE trên connection luôn đúng thứ tự các sự kiện.
func (s *Server) acquireRoom(ctx context.Context, name string) (*room, error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, errClosing
	}
	rm := s.rooms[name]
	if rm == nil {
		rm = &room{name: name, channel: RoomChannel(s.opts.Prefix, name), clients: map[*client]struct{}{}, ready: make(chan struct{})}
		s.waiters[rm.channel] = append(s.waiters[rm.channel], rm.ready)
		if err := s.pubsub.Subscribe(ctx, rm.channel); err != nil {
			s.waiters[rm.channel] = s.waiters[rm.channel][:len(s.waiters[rm.channel])-1]
			s.mu.Unlock()
			return nil, fmt.Errorf("subscribe %s: %w", rm.channel, err)
		}
		s.rooms[name] = rm
	}
	rm.refs++
	s.mu.Unlock()

	select {
	case <-rm.ready:
		return rm, nil
	case <-ctx.Done():
		s.releaseRoom(rm)
		return nil, ctx.Err()
	case <-s.baseCtx.Done():
		s.releaseRoom(rm)
		return nil, errClosing
	}
}

// releaseRoom trả chỗ. Người cuối cùng rời phòng thì instance unsubscribe channel.
func (s *Server) releaseRoom(rm *room) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseRoomLocked(rm)
}

func (s *Server) releaseRoomLocked(rm *room) {
	rm.refs--
	if rm.refs > 0 || s.rooms[rm.name] != rm {
		return
	}
	delete(s.rooms, rm.name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Lỗi ErrClosed khi đang shutdown là bình thường: đóng connection cũng hủy mọi subscription.
	if err := s.pubsub.Unsubscribe(ctx, rm.channel); err != nil && !errors.Is(err, redis.ErrClosed) {
		s.opts.OnError(fmt.Errorf("unsubscribe %s: %w", rm.channel, err))
	}
}

// ---- Fan-out: mọi message tới client đều đi qua Redis, kể cả với client cùng instance với người gửi ----

// receiveLoop là goroutine DUY NHẤT đọc từ subscriber connection. Dùng Receive thay cho Channel() vì
// Channel() nuốt xác nhận subscribe, mà ta cần xác nhận đó để biết khi nào instance thật sự sẵn sàng.
func (s *Server) receiveLoop() {
	defer s.wg.Done()
	for {
		msg, err := s.pubsub.Receive(context.Background())
		if err != nil {
			if errors.Is(err, redis.ErrClosed) || s.isClosing() {
				return
			}
			// Lỗi tạm (đứt kết nối): go-redis tự nối lại và subscribe lại, message trong lúc đứt đã mất.
			s.opts.OnError(fmt.Errorf("subscriber: %w", err))
			select {
			case <-time.After(200 * time.Millisecond):
			case <-s.baseCtx.Done():
				return
			}
			continue
		}
		switch m := msg.(type) {
		case *redis.Subscription:
			if m.Kind == "subscribe" {
				s.confirmSubscribe(m.Channel)
			}
		case *redis.Message:
			s.dispatch(m.Channel, m.Payload)
		}
	}
}

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// confirmSubscribe đánh thức người chờ SUBSCRIBE theo thứ tự. Xác nhận subscribe của một channel luôn
// về theo thứ tự lệnh, và xác nhận thừa (sau khi nối lại) khi hàng đợi rỗng thì bị bỏ qua.
func (s *Server) confirmSubscribe(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.waiters[channel]
	if len(queue) == 0 {
		return
	}
	close(queue[0])
	if len(queue) == 1 {
		delete(s.waiters, channel)
	} else {
		s.waiters[channel] = queue[1:]
	}
}

func (s *Server) dispatch(channel, payload string) {
	prefix := RoomChannel(s.opts.Prefix, "")
	name, ok := strings.CutPrefix(channel, prefix)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rm := s.rooms[name]
	if rm == nil {
		return
	}
	for c := range rm.clients {
		select {
		case c.send <- []byte(payload):
		default:
			// Chính sách client chậm: hàng đợi đầy thì hủy client này. Các client khác không bị ảnh hưởng vì
			// việc đẩy vào hàng đợi không bao giờ chặn.
			c.cancel()
		}
	}
}

// ---- WebSocket ----

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	roomName := r.URL.Query().Get("room")
	if !namePattern.MatchString(roomName) {
		writeText(w, http.StatusBadRequest, "room phải "+nameRule)
		return
	}
	name := "anon-" + randomHex(3)
	if r.URL.Query().Has("name") {
		if name = r.URL.Query().Get("name"); !namePattern.MatchString(name) {
			writeText(w, http.StatusBadRequest, "name phải "+nameRule)
			return
		}
	}
	// Origin: client không phải trình duyệt thường không gửi Origin nên được nhận. Trình duyệt luôn gửi,
	// và lab chỉ nhận origin cùng host (đúng mặc định của websocket.Accept khi OriginPatterns rỗng).
	// Production phải liệt kê origin được phép và xác thực người dùng.
	if !originAllowed(r) {
		writeText(w, http.StatusForbidden, errBadOrigin.Error())
		return
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		writeText(w, http.StatusServiceUnavailable, errClosing.Error())
		return
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	// Chỉ nhận kết nối sau khi instance đã subscribe xong: client open nghĩa là mọi message về sau tới được.
	rm, err := s.acquireRoom(r.Context(), roomName)
	if err != nil {
		if errors.Is(err, errClosing) {
			writeText(w, http.StatusServiceUnavailable, errClosing.Error())
		} else if r.Context().Err() == nil {
			s.opts.OnError(err)
			writeText(w, http.StatusServiceUnavailable, "không subscribe được channel của phòng")
		}
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		// Accept đã tự trả lỗi HTTP (ví dụ 426 khi không phải upgrade request).
		s.releaseRoom(rm)
		return
	}
	// Giới hạn kích thước message: vượt quá thì thư viện đóng connection với 1009 (Message Too Big).
	conn.SetReadLimit(s.opts.MaxMessageBytes)

	ctx, cancel := context.WithCancel(s.baseCtx)
	c := &client{name: name, room: rm, conn: conn, send: make(chan []byte, s.opts.SendQueue), ctx: ctx, cancel: cancel}
	s.mu.Lock()
	if s.closing {
		// Shutdown bắt đầu giữa lúc upgrade: không đăng ký, đóng ngay.
		s.releaseRoomLocked(rm)
		s.mu.Unlock()
		cancel()
		_ = conn.Close(websocket.StatusGoingAway, "server đang tắt")
		return
	}
	rm.clients[c] = struct{}{}
	s.clients[c] = struct{}{}
	s.mu.Unlock()

	var helpers sync.WaitGroup
	helpers.Add(2)
	go func() {
		defer helpers.Done()
		s.writeLoop(c)
	}()
	go func() {
		defer helpers.Done()
		s.heartbeat(c)
	}()

	s.readLoop(c)

	// Dọn: hủy context (dừng writer và heartbeat), đóng socket, bỏ client khỏi phòng RỒI mới chờ goroutine.
	cancel()
	_ = conn.CloseNow()
	s.removeClient(c)
	helpers.Wait()
}

// removeClient bỏ client khỏi phòng. Sau lúc này dispatch không bao giờ đẩy gì vào client nữa.
func (s *Server) removeClient(c *client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := c.room.clients[c]; !ok {
		return
	}
	delete(c.room.clients, c)
	delete(s.clients, c)
	s.releaseRoomLocked(c.room)
}

// readLoop đọc message của client và publish lên Redis. Trả về khi connection đóng hoặc có lỗi.
func (s *Server) readLoop(c *client) {
	for {
		typ, data, err := c.conn.Read(c.ctx)
		if err != nil {
			// Peer đóng (có hoặc không có close frame), context bị hủy (client chậm, heartbeat, shutdown),
			// hoặc message vượt giới hạn (thư viện đã đóng với 1009).
			return
		}
		text, ok := parseText(typ, data)
		if !ok {
			// 1003 (Unsupported Data): server nhận kiểu dữ liệu nó không chấp nhận. Chỉ connection này bị đóng.
			_ = c.conn.Close(websocket.StatusUnsupportedData, closeBadMessage)
			return
		}
		payload, err := json.Marshal(ChatMessage{Room: c.room.name, From: c.name, Text: text, TS: time.Now().UnixMilli()})
		if err != nil {
			s.opts.OnError(err)
			return
		}
		if err := s.rdb.Publish(c.ctx, c.room.channel, payload).Err(); err != nil {
			if c.ctx.Err() == nil {
				s.opts.OnError(fmt.Errorf("publish: %w", err))
				// 1011 (Internal Error): không publish được thì message sẽ mất, nên báo cho client biết.
				_ = c.conn.Close(websocket.StatusInternalError, "không gửi được message, Redis lỗi")
			}
			return
		}
	}
}

// writeLoop là goroutine duy nhất ghi xuống socket của client, lấy từ hàng đợi có giới hạn.
func (s *Server) writeLoop(c *client) {
	for {
		select {
		case <-c.ctx.Done():
			return
		case payload := <-c.send:
			ctx, cancel := context.WithTimeout(c.ctx, s.opts.WriteTimeout)
			err := c.conn.Write(ctx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				// Ghi lỗi hoặc quá WriteTimeout: client chết hoặc chậm, hủy nó.
				c.cancel()
				return
			}
		}
	}
}

// heartbeat ping định kỳ. Client không trả pong trong một chu kỳ bị hủy. Ping cần readLoop đang chạy
// để đọc pong, và readLoop luôn chạy suốt đời connection.
func (s *Server) heartbeat(c *client) {
	ticker := time.NewTicker(s.opts.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(c.ctx, s.opts.PingInterval)
			err := c.conn.Ping(ctx)
			cancel()
			if err != nil {
				c.cancel()
				return
			}
		}
	}
}

func parseText(typ websocket.MessageType, data []byte) (string, bool) {
	if typ != websocket.MessageText {
		return "", false
	}
	var parsed struct {
		Text *string `json:"text"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil || parsed.Text == nil || *parsed.Text == "" {
		return "", false
	}
	return *parsed.Text, true
}

func originAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(u.Host, r.Host)
}

// ---- HTTP: API bảng xếp hạng ----

func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ws" {
		s.handleWS(w, r)
		return
	}
	match := leaderRoute.FindStringSubmatch(r.URL.EscapedPath())
	if match == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "không có đường dẫn này"})
		return
	}
	name, err := url.PathUnescape(match[1])
	if err != nil || !namePattern.MatchString(name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name phải " + nameRule})
		return
	}
	key := LeaderboardKey(s.opts.Prefix, name)
	if match[2] == "" {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "chỉ hỗ trợ POST"})
			return
		}
		s.postScore(w, r, name, key)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "chỉ hỗ trợ GET"})
		return
	}
	s.top(w, r, name, key)
}

func (s *Server) postScore(w http.ResponseWriter, r *http.Request, name, key string) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			w.Header().Set("Connection", "close")
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "body vượt quá " + strconv.Itoa(maxBodyBytes) + " byte"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "không đọc được body"})
		return
	}
	var body struct {
		Member *string  `json:"member"`
		Score  *float64 `json:"score"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body không phải JSON hợp lệ hoặc sai kiểu dữ liệu"})
		return
	}
	if body.Member == nil || *body.Member == "" || utf8.RuneCountInString(*body.Member) > maxMemberLength {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("member phải là chuỗi dài 1 đến %d ký tự", maxMemberLength)})
		return
	}
	if body.Score == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "score phải là một số hữu hạn"})
		return
	}
	// ZADD đặt score (set), không cộng dồn: POST lại cùng member thì thay score cũ.
	if err := s.rdb.ZAdd(r.Context(), key, redis.Z{Score: *body.Score, Member: *body.Member}).Err(); err != nil {
		s.opts.OnError(err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "lỗi nội bộ, xem log của server"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "member": *body.Member, "score": *body.Score})
}

func (s *Server) top(w http.ResponseWriter, r *http.Request, name, key string) {
	n := defaultTop
	if q := r.URL.Query(); q.Has("n") {
		raw := q.Get("n")
		parsed, err := strconv.Atoi(raw)
		if !digitsOnly.MatchString(raw) || err != nil || parsed < 1 || parsed > maxTop {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("n phải là số nguyên từ 1 đến %d", maxTop)})
			return
		}
		n = parsed
	}
	// ZRANGE ... REV thay cho ZREVRANGE (đã deprecated). Score bằng nhau thì member theo thứ tự từ điển GIẢM dần.
	zs, err := s.rdb.ZRangeArgsWithScores(r.Context(), redis.ZRangeArgs{Key: key, Start: 0, Stop: int64(n - 1), Rev: true}).Result()
	if err != nil {
		s.opts.OnError(err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "lỗi nội bộ, xem log của server"})
		return
	}
	type entry struct {
		Member string  `json:"member"`
		Score  float64 `json:"score"`
	}
	entries := make([]entry, 0, len(zs))
	for _, z := range zs {
		entries = append(entries, entry{Member: fmt.Sprint(z.Member), Score: z.Score})
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "entries": entries})
}

// ---- Shutdown ----

// Shutdown đóng mọi thứ theo pha tuần tự và gom lỗi: dừng nhận kết nối, đóng WebSocket (1001), đóng hai
// connection Redis, rồi chờ mọi goroutine thoát. Idempotent: gọi nhiều lần (kể cả đồng thời) trả về cùng
// một kết quả của lần gọi đầu tiên (ctx của lần đầu quyết định thời hạn).
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() { s.shutdownErr = s.shutdown(ctx) })
	return s.shutdownErr
}

func (s *Server) shutdown(ctx context.Context) error {
	var errs []error
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()

	// Pha 1: ngừng nhận kết nối mới. WebSocket đã hijack nên không bị Shutdown của http.Server chờ.
	if err := s.http.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("http shutdown: %w", err))
	}
	// Pha 2: đóng mọi WebSocket bằng 1001 (Going Away), chờ close handshake, hủy những socket còn lại.
	// Mỗi client rời phòng qua đường dọn bình thường nên phòng cuối cùng sẽ UNSUBSCRIBE.
	s.closeClients(ctx)
	// Pha 3: đóng subscriber (đánh thức receiveLoop) rồi connection lệnh thường.
	if err := s.pubsub.Close(); err != nil {
		errs = append(errs, fmt.Errorf("đóng subscriber: %w", err))
	}
	if err := s.rdb.Close(); err != nil {
		errs = append(errs, fmt.Errorf("đóng redis: %w", err))
	}
	s.baseCancel()
	// Pha 4: mọi goroutine phải thoát.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		errs = append(errs, fmt.Errorf("goroutine chưa thoát khi hết hạn shutdown: %w", ctx.Err()))
	}
	return errors.Join(errs...)
}

func (s *Server) closeClients(ctx context.Context) {
	s.mu.Lock()
	open := make([]*client, 0, len(s.clients))
	for c := range s.clients {
		open = append(open, c)
	}
	s.mu.Unlock()

	var closers sync.WaitGroup
	for _, c := range open {
		closers.Add(1)
		go func() {
			defer closers.Done()
			// Close gửi close frame và chờ peer trả lời. Lỗi (peer đã đi mất) không quan trọng ở đây.
			_ = c.conn.Close(websocket.StatusGoingAway, "server đang tắt")
		}()
	}
	done := make(chan struct{})
	go func() {
		closers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-time.After(s.opts.CloseGrace):
	case <-ctx.Done():
	}
	// Quá hạn: hủy hẳn các socket không trả lời close handshake.
	for _, c := range open {
		c.cancel()
		_ = c.conn.CloseNow()
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// ---- Tiện ích ----

func writeText(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, message+"\n")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic("lab: crypto/rand lỗi: " + err.Error())
	}
	return hex.EncodeToString(buf)
}
