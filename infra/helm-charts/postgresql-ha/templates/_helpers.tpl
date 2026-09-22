{{- define "postgresql.fullname" -}}
{{ .Release.Name }}-{{ .Chart.Name }}
{{- end -}}

{{- define "postgresql.labels" -}}
app: postgresql
cluster-name: postgresql
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "postgresql.authSecretName" -}}
{{- if .Values.auth.existingSecret -}}
{{ .Values.auth.existingSecret }}
{{- else -}}
{{ include "postgresql.fullname" . }}-auth
{{- end -}}
{{- end -}}