package logs

import (
	"log"
)

type Logs struct {
	verbocity int
	loggerOut *log.Logger
	loggerErr *log.Logger
}

func NewLogs(verbocity int, out *log.Logger, err *log.Logger) *Logs {
	res := new(Logs)
	res.verbocity = verbocity
	res.loggerOut = out
	res.loggerErr = err
	return res
}

func (l *Logs) Errprintln(arg ...interface{}) {
	if l.verbocity > 0 {
		l.loggerErr.Println(arg...)
	}
}

func (l *Logs) Errprint(frm string, arg ...interface{}) {
	if l.verbocity > 0 {
		l.loggerErr.Printf(frm, arg...)
	}
}

func (l *Logs) Logprintln(arg ...interface{}) {
	if l.verbocity > 1 {
		l.loggerOut.Println(arg...)
	}
}

func (l *Logs) Logprint(frm string, arg ...interface{}) {
	if l.verbocity > 1 {
		l.loggerOut.Printf(frm, arg...)
	}
}
