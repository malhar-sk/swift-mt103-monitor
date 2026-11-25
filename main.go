package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"pimon/swift"
)

var (
	cpuTempGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pi_cpu_temperature_celsius",
		Help: "CPU temperature in Celsius",
	})
	pingLatencyGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pi_ping_latency_ms",
		Help: "Ping latency to 1.1.1.1 in milliseconds",
	})
)

// env returns the value of key, or def when it is unset or empty.
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getCPUTemp() (float64, error) {
	data, err := os.ReadFile("/sys/class/thermal/thermal_zone0/temp")
	if err != nil {
		return 0, err
	}
	millideg, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, err
	}
	return float64(millideg) / 1000.0, nil
}

func getPingLatency(host string) (float64, error) {
	out, err := exec.Command("ping", "-c", "1", "-W", "2", host).Output()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "time=") {
			parts := strings.Split(line, "time=")
			ms := strings.Fields(parts[1])[0]
			return strconv.ParseFloat(ms, 64)
		}
	}
	return 0, fmt.Errorf("no time= found in ping output")
}

func main() {
	ctx := context.Background()

	prometheus.MustRegister(cpuTempGauge)
	prometheus.MustRegister(pingLatencyGauge)

	otlpEndpoint := env("PIMON_OTLP_ENDPOINT", "127.0.0.1:4317")
	broker := env("PIMON_KAFKA_BROKER", "127.0.0.1:9092")
	metricsAddr := env("PIMON_METRICS_ADDR", "127.0.0.1:9100")
	failRate, err := strconv.ParseFloat(env("PIMON_FAIL_RATE", "0.15"), 64)
	if err != nil || failRate < 0 || failRate > 1 {
		panic("PIMON_FAIL_RATE must be a number between 0 and 1")
	}

	conn, err := grpc.NewClient(otlpEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}

	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		panic(err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(10*time.Second))),
	)
	defer provider.Shutdown(ctx)

	meter := provider.Meter("pimon")

	swift.InitMetrics(meter)

	cpuTemp, _ := meter.Float64ObservableGauge("pi.cpu.temperature")
	pingLatency, _ := meter.Float64ObservableGauge("pi.ping.latency")

	_, err = meter.RegisterCallback(func(ctx context.Context, o otelmetric.Observer) error {
		temp, err := getCPUTemp()
		if err == nil {
			o.ObserveFloat64(cpuTemp, temp)
			cpuTempGauge.Set(temp)
		}
		lat, err := getPingLatency("1.1.1.1")
		if err == nil {
			o.ObserveFloat64(pingLatency, lat)
			pingLatencyGauge.Set(lat)
		}
		return nil
	}, cpuTemp, pingLatency)
	if err != nil {
		panic(err)
	}

	go swift.StartMonitor(ctx, broker)
	go swift.StartProducer(ctx, broker, failRate)

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(metricsAddr, nil)

	fmt.Printf("pimon: OTLP %s, Kafka %s, /metrics on %s, fail rate %.2f\n", otlpEndpoint, broker, metricsAddr, failRate)
	select {}
}
