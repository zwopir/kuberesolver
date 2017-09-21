package kuberesolver

import (
	"net"
	"strconv"
	"sync"

	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
	"errors"
)

var (
	nextCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kuberesolver_next_calls",
		Help: "number of calls to Next()",
	}, []string{"target", "status"})
	updateSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kuberesolver_update_size",
		Help: "number of elements returned by Next()",
	}, []string{"target"})
	operations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kuberesolver_update_operations",
		Help: "number of statements in update returned by Next() partitioned by operation (delete|add)",
	}, []string{"target", "op"})
	lastCalledTS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kuberesolver_next_last_called_timestamp_seconds",
		Help: "timestamp of the latest Next() invocation",
	}, []string{"target"})
)

func init() {
	prometheus.MustRegister(nextCalls, updateSize, operations, lastCalledTS)
}

type watchResult struct {
	ep  *Event
	err error
}

// A Watcher provides name resolution updates from Kubernetes endpoints
// identified by name.
type watcher struct {
	target    targetInfo
	endpoints map[string]interface{}
	stopCh    chan struct{}
	result    chan watchResult
	sync.Mutex
	stopped bool
}

// Close closes the watcher, cleaning up any open connections.
func (w *watcher) Close() {
	close(w.stopCh)
}

// Next updates the endpoints for the name being watched.
func (w *watcher) Next() ([]*naming.Update, error) {
	lastCalledTS.WithLabelValues(w.target.target).Set(float64(time.Now().Unix()))
	updates := make([]*naming.Update, 0)
	updatedEndpoints := make(map[string]interface{})
	var ep Event

	select {
	case <-w.stopCh:
		w.Lock()
		if !w.stopped {
			w.stopped = true
		}
		w.Unlock()
		nextCalls.WithLabelValues(w.target.target, "stopped").Inc()
		return updates, nil
	case r, ok := <-w.result:
		if ok {
			if r.err == nil {
				ep = *r.ep
			} else {
				nextCalls.WithLabelValues(w.target.target, "error").Inc()
				return updates, r.err
			}
		} else {
			grpclog.Warning("kuberesolver: watcher receive channel closed")
			return updates, errors.New("receive channel closed")
		}
	}
	for _, subset := range ep.Object.Subsets {
		port := ""
		if w.target.useFirstPort {
			port = strconv.Itoa(subset.Ports[0].Port)
		} else if w.target.resolveByPortName {
			for _, p := range subset.Ports {
				if p.Name == w.target.port {
					port = strconv.Itoa(p.Port)
					break
				}
			}
		} else {
			port = w.target.port
		}

		if len(port) == 0 {
			port = strconv.Itoa(subset.Ports[0].Port)
		}
		for _, address := range subset.Addresses {
			endpoint := net.JoinHostPort(address.IP, port)
			updatedEndpoints[endpoint] = nil
		}
	}

	// Create updates to add new endpoints.
	for addr, md := range updatedEndpoints {
		if _, ok := w.endpoints[addr]; !ok {
			updates = append(updates, &naming.Update{naming.Add, addr, md})
			grpclog.Printf("kuberesolver: %s ADDED to %s", addr, w.target.target)
			operations.WithLabelValues(w.target.target, "add").Inc()
		}
	}

	// Create updates to delete old endpoints.
	for addr := range w.endpoints {
		if _, ok := updatedEndpoints[addr]; !ok {
			updates = append(updates, &naming.Update{naming.Delete, addr, nil})
			grpclog.Printf("kuberesolver: %s DELETED from %s", addr, w.target.target)
			operations.WithLabelValues(w.target.target, "delete").Inc()
		}
	}
	w.endpoints = updatedEndpoints
	nextCalls.WithLabelValues(w.target.target, "success").Inc()
	updateSize.WithLabelValues(w.target.target).Set(float64(len(updatedEndpoints)))
	return updates, nil
}
