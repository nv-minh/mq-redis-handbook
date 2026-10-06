package lab

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"time"
)

// composeProject is the compose project of this handbook. Chaos never touches a container outside it.
const composeProject = "mq-handbook"

// ChaosServices are the only services chaos may stop: the three Redis nodes of the sentinel profile.
var ChaosServices = []string{"redis-master", "redis-replica-1", "redis-replica-2"}

func composeFile() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "infra", "docker-compose.yml")
}

func compose(ctx context.Context, action, service string, extra ...string) error {
	if !slices.Contains(ChaosServices, service) {
		return fmt.Errorf("refusing to %s %q: not one of %v", action, service, ChaosServices)
	}
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	args := append([]string{"compose", "-p", composeProject, "-f", composeFile(), action}, extra...)
	args = append(args, service)
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("docker %v: %w\n%s", args, err, out)
	}
	return nil
}

// StopService stops one sentinel-profile Redis node (SIGTERM, one second grace) through docker compose.
func StopService(ctx context.Context, service string) error {
	return compose(ctx, "stop", service, "-t", "1")
}

// StartService starts a node that was stopped. It is safe to call for a node that is already running.
func StartService(ctx context.Context, service string) error {
	return compose(ctx, "start", service)
}
