package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	ctrl "sigs.k8s.io/controller-runtime"
)

// findReloadTargets scans procRoot (normally /proc) for the co-located
// synapse proxy process — argv0 basename == "synapse" — excluding the
// scanner itself (`self`). With the pod's shareProcessNamespace:true
// the operator sidecar sees synapse's PID here. Pure + procRoot-
// parameterized so it is unit-testable without real processes.
func findReloadTargets(procRoot string, self int) []int {
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(procRoot, e.Name(), "cmdline"))
		if err != nil || len(raw) == 0 {
			continue
		}
		argv0 := string(raw)
		if i := strings.IndexByte(argv0, 0); i >= 0 {
			argv0 = argv0[:i]
		}
		if argv0 == "" {
			continue
		}
		switch filepath.Base(argv0) {
		case "synapse":
			pids = append(pids, pid)
		}
	}
	return pids
}

// signalReload SIGHUPs the co-located synapse process so it
// deterministically re-reads upstreams.yaml (synapse's SIGHUP handler
// broadcasts a reload; the upstreams filewatch's reload arm re-reads
// with no debounce). Independent of inotify event types / timing.
func (r *IngressReconciler) signalReload(ctx context.Context) {
	logger := ctrl.LoggerFrom(ctx).WithName("reload")
	pids := findReloadTargets("/proc", os.Getpid())
	if len(pids) == 0 {
		logger.Info("upstreams changed but no synapse process found to SIGHUP " +
			"(shareProcessNamespace not enabled, or synapse not started yet)")
		return
	}
	for _, pid := range pids {
		if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
			logger.Error(err, "SIGHUP failed", "pid", pid)
		} else {
			logger.Info("SIGHUP → synapse reload", "pid", pid)
		}
	}
}
