{{/* Common name + labels helpers. */}}

{{- define "fjord.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fjord.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "fjord.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fjord.labels" -}}
app.kubernetes.io/name: {{ include "fjord.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "fjord.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fjord.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Broker Service DNS — the default advertised host (in-cluster clients). */}}
{{- define "fjord.serviceDns" -}}
{{- printf "%s.%s.svc.cluster.local" (include "fjord.fullname" .) .Release.Namespace -}}
{{- end -}}

{{/* Advertised host: explicit override, else the Service DNS. */}}
{{- define "fjord.advertisedHost" -}}
{{- if .Values.broker.advertisedHost -}}
{{- .Values.broker.advertisedHost -}}
{{- else -}}
{{- include "fjord.serviceDns" . -}}
{{- end -}}
{{- end -}}

{{- define "fjord.headlessName" -}}
{{- printf "%s-headless" (include "fjord.fullname" .) -}}
{{- end -}}

{{/* multiBroker peer list: id@<sts>-<i>.<headless>.<ns>.svc.cluster.local:port,…
     for every replica. Each pod resolves its own ordinal from HOSTNAME at
     startup; the peer set is identical across pods. */}}
{{- define "fjord.peers" -}}
{{- $full := include "fjord.fullname" . -}}
{{- $headless := include "fjord.headlessName" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := int .Values.broker.port -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.replicaCount) -}}
{{- $peers = append $peers (printf "%d@%s-%d.%s.%s.svc.cluster.local:%d" $i $full $i $headless $ns $port) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{- define "fjord.postgresName" -}}
{{- printf "%s-postgres" (include "fjord.fullname" .) -}}
{{- end -}}

{{- define "fjord.minioName" -}}
{{- printf "%s-minio" (include "fjord.fullname" .) -}}
{{- end -}}

{{/* Coordinator URL: explicit value, else derived from the bundled Postgres. */}}
{{- define "fjord.coordinatorUrl" -}}
{{- if .Values.coordinator.url -}}
{{- .Values.coordinator.url -}}
{{- else if .Values.postgres.enabled -}}
{{- $schema := ternary (printf "?schema=%s" .Values.coordinator.schema) "" (ne .Values.coordinator.schema "") -}}
{{- printf "postgresql://%s:%s@%s:5432/%s%s" .Values.postgres.user .Values.postgres.password (include "fjord.postgresName" .) .Values.postgres.database $schema -}}
{{- else -}}
{{- fail "coordinator.url must be set when postgres.enabled is false" -}}
{{- end -}}
{{- end -}}

{{/* S3 endpoint: explicit, else the bundled MinIO when enabled. */}}
{{- define "fjord.s3Endpoint" -}}
{{- if .Values.objectStore.endpoint -}}
{{- .Values.objectStore.endpoint -}}
{{- else if .Values.minio.enabled -}}
{{- printf "http://%s:9000" (include "fjord.minioName" .) -}}
{{- end -}}
{{- end -}}
