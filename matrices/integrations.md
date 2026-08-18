# Integration Pattern Matrix

> Generated from `integrations/*.yml`. Do not edit by hand.

| Pattern | Category | Direction | Typical protocols | Status |
|---|---|---|---|---|
| Master Data API | master-data | bidirectional | REST/JSON, GraphQL | internal-proposal |
| Transactional Command API | transaction | bidirectional | REST/JSON | internal-proposal |
| Domain Event Stream | event | outbound | webhook, Kafka, AMQP, CloudEvents | internal-proposal |
| EDI and Business Document Exchange | batch | bidirectional | EDI, AS2, SFTP | observed |
| Barcode and Data Carrier Capture | identity | inbound | GS1-128, GS1 DataMatrix, QR, RFID/EPC | observed |
| Operational Label Printing | labeling | outbound | ZPL, PDF, IPP, local print gateway | internal-proposal |
| Equipment and WCS Integration | device | bidirectional | OPC UA, TCP, vendor API, REST, WebSocket | internal-proposal |
| Operational Telemetry Stream | telemetry | inbound | MQTT, Sparkplug B, OPC UA PubSub, OTLP | internal-proposal |
