package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
)

// findReloadTargets scans procRoot (normally /proc) for the co-located
// proxy process — argv0 basename == name — excluding the scanner
// itself (`self`). With the pod's shareProcessNamespace:true the
// operator sidecar sees that PID here. Pure + procRoot/name-
// parameterized so it is unit-testable without real processes.
func findReloadTargets(procRoot string, self int, name string) []int {
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
		if filepath.Base(argv0) == name {
			pids = append(pids, pid)
		}
	}
	return pids
}

// reloadDebouncer collapses SIGHUP bursts. The leading edge fires
// immediately; any triggers within `window` of the last fire collapse
// into a single trailing fire scheduled at the end of the window — so
// rapid churn (e.g. cert-manager creating/deleting solver objects)
// does not produce a SIGHUP storm, while the final state is ALWAYS
// applied (trailing edge guarantees eventual consistency). window<=0
// disables debouncing (every trigger fires immediately).
type reloadDebouncer struct {
	mu      sync.Mutex
	window  time.Duration
	last    time.Time
	pending bool
	do      func()
}

func newReloadDebouncer(window time.Duration, do func()) *reloadDebouncer {
	return &reloadDebouncer{window: window, do: do}
}

func (d *reloadDebouncer) trigger() {
	d.mu.Lock()
	now := time.Now()
	if d.window <= 0 || now.Sub(d.last) >= d.window {
		d.last = now
		d.mu.Unlock()
		d.do()
		return
	}
	if d.pending {
		d.mu.Unlock()
		return
	}
	d.pending = true
	wait := d.window - now.Sub(d.last)
	d.mu.Unlock()
	time.AfterFunc(wait, func() {
		d.mu.Lock()
		d.pending = false
		d.last = time.Now()
		d.mu.Unlock()
		d.do()
	})
}

// signalReload SIGHUPs the co-located synapse process so it
// deterministically re-reads upstreams.yaml (synapse's SIGHUP handler
// broadcasts a reload; the upstreams filewatch's reload arm re-reads
// with no debounce). Independent of inotify event types / timing.
// Bursts are coalesced via reloadDebouncer.
func (r *IngressReconciler) signalReload(ctx context.Context) {
	r.reloadOnce.Do(func() {
		r.reload = newReloadDebouncer(r.ReloadDebounce, func() {
			r.doReload(ctrl.Log.WithName("reload"))
		})
	})
	_ = ctx
	r.reload.trigger()
}

func (r *IngressReconciler) doReload(logger logr.Logger) {
	name := r.ReloadProcessName
	if name == "" {
		name = "synapse"
	}
	pids := findReloadTargets("/proc", os.Getpid(), name)
	if len(pids) == 0 {
		logger.Info("upstreams changed but no target process found to SIGHUP "+
			"(shareProcessNamespace not enabled, or process not started yet)", "process", name)
		return
	}
	for _, pid := range pids {
		if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
			logger.Error(err, "SIGHUP failed", "pid", pid, "process", name)
		} else {
			logger.Info("SIGHUP → reload", "pid", pid, "process", name)
			mReloadTotal.Inc()
		}
	}
}
