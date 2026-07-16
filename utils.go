package go_queue

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/levskiy0/go-queue/contract"
)

const minRateLimitRetryDelay = 100 * time.Millisecond

func jobs2Tasks(jobs []contract.Job, log *slog.Logger, queue string, rateLimit *contract.RateLimit) (map[string]any, error) {
	if log == nil {
		log = slog.Default()
	}

	var limiters *keyedLimiters
	if rateLimit != nil {
		if rateLimit.Limit < 1 || rateLimit.Per <= 0 {
			log.Warn("rate limit disabled: invalid configuration", "queue", queue, "limit", rateLimit.Limit, "per", rateLimit.Per)
		} else {
			limiters = newKeyedLimiters(rateLimit)
		}
	}

	result := make(map[string]any)

	for _, job := range jobs {
		if job.Signature() == "" {
			return nil, fmt.Errorf("empty Job signature")
		}

		if result[job.Signature()] != nil {
			return nil, fmt.Errorf("duplicate Job signature: %s", job.Signature())
		}

		result[job.Signature()] = wrapJob(job, log, queue, rateLimit, limiters)
	}

	return result, nil
}

func wrapJob(job contract.Job, log *slog.Logger, queue string, rateLimit *contract.RateLimit, limiters *keyedLimiters) func(args ...any) error {
	return func(args ...any) error {
		if limiters != nil {
			key := queue
			if rateLimit.Key != nil {
				key = queue + ":" + rateLimit.Key(args)
			}

			if delay := limiters.reserveDelay(key); delay > 0 {
				if delay < minRateLimitRetryDelay {
					delay = minRateLimitRetryDelay
				}

				return tasks.NewErrRetryTaskLater("rate limited", delay)
			}
		}

		err := job.Handle(args...)
		if err == nil {
			return nil
		}

		if noRetryJob, ok := job.(contract.JobWithNoRetry); ok && noRetryJob.NoRetry(err) {
			log.Warn("job failed, no retry", "signature", job.Signature(), "error", err)

			return nil
		}

		return err
	}
}
