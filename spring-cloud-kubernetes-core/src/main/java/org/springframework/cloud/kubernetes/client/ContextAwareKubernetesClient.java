package org.springframework.cloud.kubernetes.client;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.internal.EndpointsOperationsImpl;
import io.fabric8.kubernetes.client.dsl.internal.ServiceOperationsImpl;
import org.springframework.cloud.kubernetes.KubernetesClientProperties;
import org.springframework.util.StringUtils;

import java.util.Map;

public class ContextAwareKubernetesClient extends DefaultKubernetesClient {

    private final KubernetesClientProperties clientProperties;
    private final Map<String, String> labels;

	public ContextAwareKubernetesClient(KubernetesClientProperties clientProperties, Map<String, String> labels) {
		this.clientProperties = clientProperties;
		this.labels = labels;
	}

	@Override
    public MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> services() {
        ServiceOperationsImpl serviceOperations = new ServiceOperationsImpl(httpClient,
            getConfiguration(), getNamespace());
        serviceOperations.withLabels(labels);

        return serviceOperations;
    }

    @Override
    public MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> endpoints() {
        if (StringUtils.isEmpty(getNamespace())) {
            EndpointsOperationsImpl endpointsOperations = new EndpointsOperationsImpl(httpClient,
                getConfiguration(), getNamespace());
            endpointsOperations.withLabels(labels);

            return (MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>>) endpointsOperations
                .inAnyNamespace();
        }

        return super.endpoints();
    }

    @Override
    public String getNamespace() {
        return clientProperties.getNamespace();
    }
}
