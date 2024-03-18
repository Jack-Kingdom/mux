package mux

import "fmt"

type Logger interface {
	Debugf(_ string, _ ...interface{})
	Infof(_ string, _ ...interface{})
	Warnf(_ string, _ ...interface{})
	Errorf(_ string, _ ...interface{})
}

type noopLoggerImpl struct{}

func (*noopLoggerImpl) Debugf(_ string, _ ...interface{}) {}
func (*noopLoggerImpl) Infof(_ string, _ ...interface{})  {}
func (*noopLoggerImpl) Warnf(_ string, _ ...interface{})  {}
func (*noopLoggerImpl) Errorf(_ string, _ ...interface{}) {}

type standardLoggerImpl struct{}

func (*standardLoggerImpl) Debugf(a string, b ...interface{}) { fmt.Printf("[DEBUG] "+a+"\n", b...) }
func (*standardLoggerImpl) Infof(a string, b ...interface{})  { fmt.Printf("[INFO]  "+a+"\n", b...) }
func (*standardLoggerImpl) Warnf(a string, b ...interface{})  { fmt.Printf("[WARN]  "+a+"\n", b...) }
func (*standardLoggerImpl) Errorf(a string, b ...interface{}) { fmt.Printf("[ERROR] "+a+"\n", b...) }

var (
	NoopLogger     = &noopLoggerImpl{}
	StandardLogger = &standardLoggerImpl{}
)
