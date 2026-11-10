{{- define "postgres.labels" -}}
app.kubernetes.io/name: postgres
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
lab.kafka/tier: platform
{{- end -}}

{{- define "postgres.selector" -}}
app.kubernetes.io/name: postgres
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Credentials as a dict key -> base64 value. Existing values (looked up from the Secret in the home
namespace) win, so passwords are generated exactly once and survive every `helmfile sync`.
Memoised in .Values so every template sees the same random values within one render.
*/}}
{{- define "postgres.credentials" -}}
{{- if not (hasKey .Values "__credentials") -}}
{{- $home := first .Values.secretNamespaces -}}
{{- $existing := (lookup "v1" "Secret" $home "postgres-credentials").data | default dict -}}
{{- $creds := dict -}}
{{- range $name := prepend .Values.services "postgres" -}}
{{- $_ := set $creds $name (get $existing $name | default (randAlphaNum 24 | b64enc)) -}}
{{- end -}}
{{- $_ := set .Values "__credentials" $creds -}}
{{- end -}}
{{- toYaml (get .Values "__credentials") -}}
{{- end -}}
