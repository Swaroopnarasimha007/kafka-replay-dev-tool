# Kafka v1 handover

Implemented a standalone Maven Java lab under kafka_v1_implementation with Kafka 3.9.1 server libraries, a single KRaft broker, orders + orders.DLT (three partitions each), two finite producers, two consumer-group members, and the console receipt topic. Configuration is hot-reloaded with validation, phase-specific allowed/forbidden failures, business attributes, producer controls, consumer controls and actual broker topic limits. Helpers install/build, start, stop and apply scenario presets.

Console lab profile uses a generated Admin login and H2 locally; Compose uses PostgreSQL. The cluster selector offers Kafka local demo and Simulation. Real browsing, source manifests, transaction receipts and metrics route by selected profile. The synthetic banner is removed. Existing live OIDC behavior is retained; lab + live cannot be combined.

Validation: 33 existing tests passed, no new tests added. Both packaged Java applications built. Native launch and managed stop/restart worked. Real Kafka integration verified: 25 committed replay outputs with 25 downstream successes; destination-size rejection blocked a 60-record job with zero committed outputs; one committed canary was rejected by its consumer and dead-lettered again; an amount threshold derived 30 validation failures without explicit error tags. Evidence is docs/evidence/kafka-lab-verification.json. Existing packaged-JAR smoke passed.

The lab is left running on port 8088 with broker bootstrap localhost:19092. Login is generated in kafka_v1_implementation/data/lab-settings.json, which is ignored by Git. Connection metadata is in that directory's connection.json. Local verification jobs and records remain for review. Configuration has been restored to its documented defaults.

Cloud Compose, Dockerfiles, PostgreSQL storage, private broker advertisement kafka:9092, and Caddy HTTPS configuration are included. Compose YAML parsed successfully. Docker Engine is unavailable here, so container execution, PostgreSQL integration for this stack, DNS, certificate issuance and actual remote deployment remain unverified. No cloud resources were created.

Read kafka_v1_implementation/README.md for exact local/cloud startup, connection details, presets and limitations. Stop the running lab before rebuilding JARs on Windows. For future work, verify the Compose stack on the target host before treating it as deployed.
