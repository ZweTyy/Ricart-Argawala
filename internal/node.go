package internal

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	pb "github.com/ZweTyy/Ricart-Argawala/internal/pb"
	"google.golang.org/grpc"
)

type State int

const (
	RELEASED State = iota
	WANTED
	HELD
)

type Peer struct {
	ID      int
	Address string
}

type Node struct {
	pb.UnimplementedRicartAgrawalaServer

	ID      int
	Addr    string
	Peers   []Peer
	mu      sync.Mutex
	clock   int64
	state   State
	requestTS int64
	pending   map[int]struct{}
	deferred  map[int]struct{}
	clients   map[int]pb.RicartAgrawalaClient
}

func NewNode(id int, addr string, peers []Peer) *Node {
	// Remove self if present
	filtered := make([]Peer, 0, len(peers))
	for _, p := range peers {
		if p.ID != id {
			filtered = append(filtered, p)
		}
	}
	return &Node{
		ID:       id,
		Addr:     addr,
		Peers:    filtered,
		pending:  map[int]struct{}{},
		deferred: map[int]struct{}{},
		clients:  map[int]pb.RicartAgrawalaClient{},
	}
}

func jlog(event string, fields map[string]any) {
	ts := time.Now().Format("15:04:05.000")
	node, _ := fields["node"]
	text := fmt.Sprintf("[%s] %-12s | node=%v", ts, event, node)

	for k, v := range fields {
		if k == "node" {
			continue
		}
		text += fmt.Sprintf(" %s=%v", k, v)
	}

	log.Println(text)
}

func (n *Node) tick(peerTS int64) int64 {
	if peerTS > n.clock {
		n.clock = peerTS
	}
	n.clock++
	return n.clock
}

func (n *Node) dialAll() error {
	for _, p := range n.Peers {
		if _, ok := n.clients[p.ID]; ok {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, p.Address, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return fmt.Errorf("dial %d %s: %w", p.ID, p.Address, err)
		}
		n.clients[p.ID] = pb.NewRicartAgrawalaClient(conn)
	}
	return nil
}

func (n *Node) RequestCSBroadcast(reqID string) error {
	n.mu.Lock()
	if n.state != RELEASED {
		n.mu.Unlock()
		return fmt.Errorf("node %d not RELEASED (state=%d)", n.ID, n.state)
	}
	n.state = WANTED
	ts := n.tick(0)
	n.requestTS = ts
	n.pending = map[int]struct{}{}
	for _, p := range n.Peers {
		n.pending[p.ID] = struct{}{}
	}
	n.deferred = map[int]struct{}{}
	n.mu.Unlock()

	if err := n.dialAll(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, p := range n.Peers {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			jlog("send_request", map[string]any{
				"node": n.ID, "peer": p.ID, "req_id": reqID, "clock": ts,
			})
			_, err := n.clients[p.ID].RequestCS(
				context.Background(),
				&pb.Request{RequesterId: int32(n.ID), Timestamp: ts, RequestId: reqID},
			)
			if err != nil {
				jlog("send_request_error", map[string]any{
					"node": n.ID, "peer": p.ID, "err": err.Error(),
				})
			}
		}()
	}
	wg.Wait()
	return nil
}

func (n *Node) RequestCS(ctx context.Context, r *pb.Request) (*pb.Ack, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tick(r.Timestamp)

	grant := false
	switch n.state {
	case RELEASED:
		grant = true
	case WANTED:
		myPair := []int64{n.requestTS, int64(n.ID)}
		other := []int64{r.Timestamp, int64(r.RequesterId)}
		grant = less(other, myPair)
	case HELD:
		grant = false
	}

	peer := int(r.RequesterId)
	if grant {
		go n.sendReply(peer, r.RequestId)
		jlog("recv_request", map[string]any{
			"node": n.ID, "peer": peer, "req_id": r.RequestId,
			"clock": n.clock, "action": "grant",
		})
	} else {
		n.deferred[peer] = struct{}{}
		jlog("recv_request", map[string]any{
			"node": n.ID, "peer": peer, "req_id": r.RequestId,
			"clock": n.clock, "action": "defer",
		})
	}
	return &pb.Ack{Ok: true}, nil
}

func less(a, b []int64) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

func (n *Node) sendReply(to int, reqID string) {
	if err := n.dialAll(); err != nil {
		jlog("dial_error", map[string]any{"node": n.ID, "err": err.Error()})
		return
	}
	ts := func() int64 {
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.tick(0)
	}()
	jlog("send_reply", map[string]any{
		"node": n.ID, "peer": to, "req_id": reqID, "clock": ts,
	})
	_, err := n.clients[to].ReplyCS(
		context.Background(),
		&pb.Reply{ReplierId: int32(n.ID), Timestamp: ts, RequestId: reqID},
	)
	if err != nil {
		jlog("send_reply_error", map[string]any{
			"node": n.ID, "peer": to, "err": err.Error(),
		})
	}
}

func (n *Node) ReplyCS(ctx context.Context, rep *pb.Reply) (*pb.Ack, error) {
	n.mu.Lock()
	n.tick(rep.Timestamp)
	if n.state == WANTED {
		delete(n.pending, int(rep.ReplierId))
	}
	remaining := len(n.pending)
	clock := n.clock
	n.mu.Unlock()

	jlog("recv_reply", map[string]any{
		"node": n.ID, "peer": int(rep.ReplierId),
		"req_id": rep.RequestId, "clock": clock, "remaining": remaining,
	})

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == WANTED && len(n.pending) == 0 {
		n.state = HELD
		jlog("enter_cs", map[string]any{
			"node": n.ID, "req_id": rep.RequestId, "clock": n.clock,
		})
		n.mu.Unlock()
		time.Sleep(600 * time.Millisecond)
		n.mu.Lock()
		n.state = RELEASED
		jlog("exit_cs", map[string]any{
			"node": n.ID, "req_id": rep.RequestId, "clock": n.clock,
		})
		ids := make([]int, 0, len(n.deferred))
		for id := range n.deferred {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for _, id := range ids {
			delete(n.deferred, id)
			go n.sendReply(id, rep.RequestId)
		}
	}
	return &pb.Ack{Ok: true}, nil
}

func (n *Node) Serve() error {
	lis, err := net.Listen("tcp", n.Addr)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterRicartAgrawalaServer(s, n)
	jlog("server_started", map[string]any{"node": n.ID, "addr": n.Addr})
	return s.Serve(lis)
}

