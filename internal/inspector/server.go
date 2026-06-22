package inspector

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
)

func NewServer(cfg Config, reg *prometheus.Registry, logger *slog.Logger) (*http.Server, error) {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	inspector, err := New(cfg, reg, logger)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/-/healthy", okHandler)
	mux.HandleFunc("/-/ready", okHandler)
	mux.HandleFunc("/api/v1/receive", inspector.receiveHTTP)
	mux.HandleFunc("/receive", inspector.receiveHTTP)

	return &http.Server{
		Addr:              inspector.cfg.ListenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}, nil
}

func okHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

func (i *Inspector) receiveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	identity := i.IdentityFromHeaders(r.Header)
	body, err := readLimited(r.Body, i.cfg.MaxBodyBytes)
	if err != nil {
		i.ObserveRequest(identity, "too_large", len(body))
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}

	decodeStart := i.clock()
	defer func() {
		i.metrics.decodeDuration.Observe(i.clock().Sub(decodeStart).Seconds())
	}()
	decodedLen, err := snappy.DecodedLen(body)
	if err != nil {
		i.ObserveDecodeError("snappy")
		i.ObserveRequest(identity, "decode_error", len(body))
		http.Error(w, fmt.Errorf("snappy decode error: %w", err).Error(), http.StatusBadRequest)
		return
	}
	if int64(decodedLen) > i.cfg.MaxBodyBytes {
		i.ObserveRequest(identity, "too_large", len(body))
		http.Error(w, "decoded write request too large", http.StatusRequestEntityTooLarge)
		return
	}
	decoded, err := snappy.Decode(nil, body)
	if err != nil {
		i.ObserveDecodeError("snappy")
		i.ObserveRequest(identity, "decode_error", len(body))
		http.Error(w, fmt.Errorf("snappy decode error: %w", err).Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(decoded, &req); err != nil {
		i.ObserveDecodeError("protobuf")
		i.ObserveRequest(identity, "decode_error", len(body))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result := i.Inspect(identity, &req)
	requestResult := "ok"
	if result.BadData {
		requestResult = "bad_data"
	} else if result.TotalSamples == 0 && len(req.Timeseries) == 0 {
		requestResult = "empty"
	}
	i.ObserveRequest(identity, requestResult, len(body))

	w.WriteHeader(http.StatusNoContent)
}

func readLimited(r io.Reader, maxBytes int64) ([]byte, error) {
	var buf bytes.Buffer
	limit := maxBytes + 1
	if limit <= 0 {
		limit = 1
	}
	_, err := io.Copy(&buf, io.LimitReader(r, limit))
	if err != nil {
		return buf.Bytes(), err
	}
	if int64(buf.Len()) > maxBytes {
		return buf.Bytes(), fmt.Errorf("write request too large")
	}
	return buf.Bytes(), nil
}
