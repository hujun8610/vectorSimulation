.PHONY: build run clean

build:
	@echo "编译应用..."
	go build -o logProcessor cmd/main.go

run: build
	@echo "运行应用..."
	./logProcessor

clean:
	@echo "清理构建文件..."
	rm -f logProcessor
	go clean

dev:
	@echo "开发模式运行..."
	go run cmd/main.go
