{{- define "MODULE.fullname" -}}
{{ .Chart.Name }}
{{- end -}}

{{- define "MODULE.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
