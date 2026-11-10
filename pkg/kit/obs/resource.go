package obs

import (
	"go.opentelemetry.io/otel/sdk/resource"
)

func serviceResource(service string) *resource.Resource {
	return resource.NewSchemaless(serviceAttrs(service)...)
}
