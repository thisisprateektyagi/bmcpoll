package errors

import "fmt"

type Err struct {
	Code    int
	Message string
}

type PollError struct {
	Err   Err
	Cause error
}

func (p *PollError) Error() string {
	if p.Cause != nil {
		return fmt.Sprintf("[%d] %s: %v", p.Err.Code, p.Err.Message, p.Cause)
	}
	return fmt.Sprintf("[%d] %s", p.Err.Code, p.Err.Message)
}

func (p *PollError) Unwrap() error {
	return p.Cause
}

func newPollError(e Err, cause error) *PollError {
	return &PollError{Err: e, Cause: cause}
}
