package errors

var (
	InvalidTargetsError = Err{
		Code:    3000,
		Message: "invalid targets",
	}

	ReadTargetsFailedError = Err{
		Code:    3001,
		Message: "failed to read targets",
	}

	TimeoutError = Err{
		Code:    3002,
		Message: "per-call timeout exceeded",
	}

	NonRetryableError = Err{
		Code:    3003,
		Message: "non-retryable response from target",
	}

	MaxRetriesExceededError = Err{
		Code:    3004,
		Message: "max retries exceeded",
	}

	CancelledError = Err{
		Code:    3005,
		Message: "cancelled before completion",
	}
)
