module go.opentelemetry.io/collector/internal/telemetrybufferprocessor

go 1.23.0

require (
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/component v1.28.1
	go.opentelemetry.io/collector/consumer v1.28.1
	go.opentelemetry.io/collector/pdata v1.28.1
	go.opentelemetry.io/collector/processor v1.28.1
	go.uber.org/zap v1.27.0
)

replace go.opentelemetry.io/collector/component => ../../component

replace go.opentelemetry.io/collector/consumer => ../../consumer

replace go.opentelemetry.io/collector/pdata => ../../pdata

replace go.opentelemetry.io/collector/processor => ../../processor