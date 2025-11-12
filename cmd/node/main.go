package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	core "github.com/ZweTyy/Ricart-Argawala/internal"
)

type peersFile struct {
	Self  struct {
		ID      int
		Address string
	} `json:"self"`
	Peers []struct {
		ID      int
		Address string
	} `json:"peers"`
}

func loadPeers(path string) (id int, addr string, peers []core.Peer, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var pf peersFile
	if err = json.Unmarshal(b, &pf); err != nil {
		return
	}
	for _, p := range pf.Peers {
		peers = append(peers, core.Peer{ID: p.ID, Address: p.Address})
	}
	return pf.Self.ID, pf.Self.Address, peers, nil
}

func main() {
	peersPath := flag.String("peers", "peers.node1.json", "path to peers config")
	autoReq := flag.Bool("request", false, "immediately request CS once started")
	flag.Parse()

	id, addr, peers, err := loadPeers(*peersPath)
	if err != nil {
		log.Fatalf("load peers: %v", err)
	}

	n := core.NewNode(id, addr, peers)
	go func() {
		if err := n.Serve(); err != nil {
			log.Fatal(err)
		}
	}()

	if *autoReq {
		time.Sleep(800 * time.Millisecond)
		_ = n.RequestCSBroadcast(time.Now().Format("20060102T150405.000"))
	}

	<-context.Background().Done()
}
