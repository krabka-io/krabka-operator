{{- define "krabka-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "krabka-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "krabka-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "krabka-operator.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "krabka-operator.commonLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- /*
The operator Deployment, its pods, the Service and the ServiceMonitor use
these labels. The component value keeps other pods of the release, such as the
CA-renewal Job pods, out of the Deployment and Service selectors.
*/ -}}
{{- define "krabka-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "krabka-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end -}}

{{- define "krabka-operator.labels" -}}
{{ include "krabka-operator.commonLabels" . }}
{{ include "krabka-operator.selectorLabels" . }}
{{- end -}}

{{- /*
The CA-renewal CronJob, its Job pods, and its RBAC objects use these labels.
*/ -}}
{{- define "krabka-operator.caRenewalSelectorLabels" -}}
app.kubernetes.io/name: {{ include "krabka-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: ca-renewal
{{- end -}}

{{- define "krabka-operator.caRenewalLabels" -}}
{{ include "krabka-operator.commonLabels" . }}
{{ include "krabka-operator.caRenewalSelectorLabels" . }}
{{- end -}}
