PROTO_DIR=proto
OUT_DIR=internal/pb
PROTO=$(PROTO_DIR)/program.proto

.PHONY: proto run1 run2 run3

proto:
	protoc -I $(PROTO_DIR) \
		--go_out=$(OUT_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO)

run1:
	go run ./cmd/node --peers peers.node1.json --request

run2:
	go run ./cmd/node --peers peers.node2.json --request

run3:
	go run ./cmd/node --peers peers.node3.json --request
