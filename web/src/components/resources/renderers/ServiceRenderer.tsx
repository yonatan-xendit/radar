import { useMemo } from 'react'
import { ServiceRenderer as BaseServiceRenderer } from '@skyhook-io/k8s-ui/components/resources/renderers/ServiceRenderer'
import { PortForwardInlineButton } from '../../portforward/PortForwardButton'
import { useResources } from '../../../api/client'
import type { ResourceRef } from '../../../types'

interface ServiceRendererProps {
  data: any
  onCopy: (text: string, label: string) => void
  copied: string | null
  onNavigate?: (ref: ResourceRef) => void
}

export function ServiceRenderer({ data, onCopy, copied, onNavigate }: ServiceRendererProps) {
  const namespace = data.metadata?.namespace
  const serviceName = data.metadata?.name
  const spec = data.spec || {}
  const shouldLoadEndpointSlices = Boolean(
    namespace &&
    serviceName &&
    spec.type !== 'ExternalName' &&
    (!spec.selector || Object.keys(spec.selector).length === 0)
  )
  const { data: endpointSlices, isLoading: endpointSlicesLoading } = useResources<any>(
    'endpointslices',
    namespace,
    'discovery.k8s.io',
    { enabled: shouldLoadEndpointSlices, refetchInterval: 30000 }
  )
  const matchingEndpointSlices = useMemo(
    () => (endpointSlices || []).filter((slice: any) => slice.metadata?.labels?.['kubernetes.io/service-name'] === serviceName),
    [endpointSlices, serviceName]
  )

  return (
    <BaseServiceRenderer
      data={data}
      onCopy={onCopy}
      copied={copied}
      endpointSlices={matchingEndpointSlices}
      endpointSlicesLoading={endpointSlicesLoading}
      onNavigate={onNavigate}
      renderPortAction={({ namespace, serviceName, port, protocol }) => (
        <PortForwardInlineButton
          namespace={namespace}
          serviceName={serviceName}
          port={port}
          protocol={protocol}
        />
      )}
    />
  )
}
