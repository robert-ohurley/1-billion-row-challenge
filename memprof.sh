#!/usr/bin/bash 

go run main.go -memprofile=mem.prof && go tool pprof -http=:8080 mem.prof
