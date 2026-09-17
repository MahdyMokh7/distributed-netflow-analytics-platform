{{- define "postgresql.fullname" -}}
{{ .Release.Name }}-{{ .Chart.Name }}
{{- end -}}

{{- define "postgresql.labels" -}}
app: postgresql
cluster-name: postgresql
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
