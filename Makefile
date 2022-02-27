VER=`cat VERSION`

build: Dockerfile cryptostore.py
	docker build . -t ghcr.io/bmoscon/cryptostore:latest

release: build
	docker build . -t ghcr.io/bmoscon/cryptostore:${VER}
