package sim

import (
	"fmt"
	"io"
	"os"
)

// Logger 带逻辑时间戳输出诊断信息，重点打印任期、多数派、提交位置
// 等判定依据；通过 debug 开关区分 INFO/DEBUG 两级，保证失败原因可区分。
type Logger struct {
	w     io.Writer
	debug bool
	tick  int64
}

// NewLogger 创建模拟器日志；w 为 nil 时输出到标准输出。
func NewLogger(w io.Writer, debug bool) *Logger {
	if w == nil {
		w = os.Stdout
	}
	return &Logger{w: w, debug: debug}
}

func (l *Logger) setTick(t int64) { l.tick = t }

// Infof 输出始终可见的关键事件。
func (l *Logger) Infof(format string, args ...any) {
	fmt.Fprintf(l.w, "[t=%05d] "+format+"\n", append([]any{l.tick}, args...)...)
}

// Debugf 输出细粒度判定依据（计票、回退、阻断、提交推进等）。
func (l *Logger) Debugf(format string, args ...any) {
	if l.debug {
		fmt.Fprintf(l.w, "[t=%05d]   "+format+"\n", append([]any{l.tick}, args...)...)
	}
}

// Logf 让 *Logger 同时满足 raft.Logger：节点内部日志按 DEBUG 输出。
func (l *Logger) Logf(format string, args ...any) {
	l.Debugf(format, args...)
}
