module go.opentelemetry.io/collector/internal/telemetryqueryextension

go 1.23.0

require (
	github.com/golang/protobuf v1.5.4
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/component v1.28.1
	go.opentelemetry.io/collector/config/configgrpc v1.28.1
	go.opentelemetry.io/collector/config/confignet v1.28.1
	go.opentelemetry.io/collector/extension v1.28.1
	go.opentelemetry.io/collector/internal/telemetrybufferprocessor v0.0.0
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.71.0
	google.golang.org/protobuf v1.36.6
)

replace go.opentelemetry.io/collector/component => ../../component

replace go.opentelemetry.io/collector/extension => ../../extension

replace go.opentelemetry.io/collector/config/configgrpc => ../../config/configgrpc

replace go.opentelemetry.io/collector/config/confignet => ../../config/confignet

replace go.opentelemetry.io/collector/internal/telemetrybufferprocessor => ../telemetrybufferprocessor