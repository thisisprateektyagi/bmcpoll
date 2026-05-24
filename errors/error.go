package errors

func CreateInvalidTargetsError(cause error) *PollError {
	return newPollError(InvalidTargetsError, cause)
}

func CreateReadTargetsFailedError(cause error) *PollError {
	return newPollError(ReadTargetsFailedError, cause)
}

func CreateTimeoutError(cause error) *PollError {
	return newPollError(TimeoutError, cause)
}

func CreateNonRetryableError(cause error) *PollError {
	return newPollError(NonRetryableError, cause)
}

func CreateMaxRetriesExceededError(cause error) *PollError {
	return newPollError(MaxRetriesExceededError, cause)
}

func CreateCancelledError(cause error) *PollError {
	return newPollError(CancelledError, cause)
}
