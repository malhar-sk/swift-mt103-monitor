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

    otelmetric "go.opentelemetry.io/otel/metric"
    "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "pimon/swift"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
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

    conn, err := grpc.NewClient("100.114.198.35:4317",
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

    go swift.StartMonitor(ctx, "100.114.198.35:9092")
    go swift.StartProducer(ctx, "100.114.198.35:9092", 0.15)

    http.Handle("/metrics", promhttp.Handler())
    go http.ListenAndServe("100.114.198.35:9100", nil)

    fmt.Println("pimon pushing to OTel collector at 100.114.198.35:4317")
    select {}
}
