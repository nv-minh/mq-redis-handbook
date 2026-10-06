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

// composeProject là compose project của handbook. Chaos không bao giờ đụng tới container nằm ngoài project này.
const composeProject = "mq-handbook"

// ChaosServices là các service duy nhất chaos được phép dừng: ba node Redis của profile sentinel.
var ChaosServices = []string{"redis-master", "redis-replica-1", "redis-replica-2"}

func composeFile() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "infra", "docker-compose.yml")
}

func compose(ctx context.Context, action, service string, extra ...string) error {
	if !slices.Contains(ChaosServices, service) {
		return fmt.Errorf("từ chối %s %q: chỉ được phép với %v", action, service, ChaosServices)
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

// StopService dừng một node Redis của profile sentinel qua docker compose (SIGTERM, chờ tối đa một giây).
func StopService(ctx context.Context, service string) error {
	return compose(ctx, "stop", service, "-t", "1")
}

// StartService bật lại node đã dừng. Gọi cho node đang chạy cũng an toàn.
func StartService(ctx context.Context, service string) error {
	return compose(ctx, "start", service)
}
