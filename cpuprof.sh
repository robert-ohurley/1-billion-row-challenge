#!/usr/bin/bash 

go run main.go -cpuprofile=cpu.prof && go tool pprof -http=:8080 cpu.prof
