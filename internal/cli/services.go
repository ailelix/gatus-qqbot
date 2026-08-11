package cli

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type readyService interface {
	Ready() <-chan struct{}
	Run(context.Context) error
}

type serviceResult struct {
	name string
	err  error
}

func runServeServices(
	ctx context.Context,
	gateway readyService,
	gatewayReadyTimeout time.Duration,
	runHTTP func(context.Context) error,
) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan serviceResult, 2)
	go func() {
		results <- serviceResult{name: "QQ gateway", err: gateway.Run(runCtx)}
	}()

	timer := time.NewTimer(gatewayReadyTimeout)
	defer timer.Stop()
	select {
	case <-gateway.Ready():
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	case result := <-results:
		if ctx.Err() != nil {
			return shutdownError(result.err)
		}
		return serviceFailure(result)
	case <-ctx.Done():
		cancel()
		return shutdownError((<-results).err)
	case <-timer.C:
		cancel()
		cleanupErr := shutdownError((<-results).err)
		return errors.Join(
			fmt.Errorf("QQ gateway did not become ready within %s", gatewayReadyTimeout),
			cleanupErr,
		)
	}

	go func() {
		results <- serviceResult{name: "HTTP server", err: runHTTP(runCtx)}
	}()

	select {
	case <-ctx.Done():
		cancel()
		first := <-results
		second := <-results
		return errors.Join(shutdownError(first.err), shutdownError(second.err))
	case first := <-results:
		parentStopped := ctx.Err() != nil
		cancel()
		second := <-results
		if parentStopped {
			return errors.Join(shutdownError(first.err), shutdownError(second.err))
		}
		return errors.Join(serviceFailure(first), shutdownError(second.err))
	}
}

func serviceFailure(result serviceResult) error {
	if result.err == nil {
		return fmt.Errorf("%s stopped unexpectedly", result.name)
	}
	return result.err
}

func shutdownError(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
