package raft

import (
	"log"
	"os"
)

const Debug = 2 // 0 不打印日志，1 打印日志但不重定向，2 重定向日志输出到 raft.log 文件

func initLogFile() {
	if (Debug == 2) { // 交互程序重定向日志输出来方便日志展示
		log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
		logFile, err := os.OpenFile("./raft.log", os.O_WRONLY | os.O_APPEND, 0644)
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(logFile)
	}
}

func LogInfo(format string, a ...interface{}) (n int, err error) {
	if Debug != 0 {
		log.Printf(format, a...)
	}
	return
}
