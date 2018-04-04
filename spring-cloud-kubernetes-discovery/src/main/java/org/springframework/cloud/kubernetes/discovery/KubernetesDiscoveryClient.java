/*
 *   Copyright (C) 2016 to the original authors.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.springframework.cloud.kubernetes.discovery;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

public class KubernetesDiscoveryClient implements DiscoveryClient {

	private static final Log log = LogFactory.getLog(KubernetesDiscoveryClient.class);

	private static final String HOSTNAME = "HOSTNAME";
	private static final String METADATA_FIELD_NAME = "metadata.name";

	private KubernetesClient client;
	private KubernetesDiscoveryProperties properties;

	public KubernetesDiscoveryClient(KubernetesClient client,
									 KubernetesDiscoveryProperties kubernetesDiscoveryProperties) {
		this.client = client;
		this.properties = properties;
	}

	public KubernetesClient getClient() {
		return client;
	}

	public void setClient(KubernetesClient client) {
		this.client = client;
	}

	@Override
	public String description() {
		return "Kubernetes Discovery Client";
	}


	public ServiceInstance getLocalServiceInstance() {
		String serviceName = properties.getServiceName();
		String podName = System.getenv(HOSTNAME);

		ServiceInstance defaultInstance = new DefaultServiceInstance(serviceName, "localhost", 8080,
			false);

		Endpoints endpoints = getServiceEndpoints(serviceName);
		if (Utils.isNullOrEmpty(podName) || endpoints == null) {
			return defaultInstance;
		}

		try {
			return endpoints
				.getSubsets()
				.stream()
				.filter(s -> s.getAddresses().get(0).getTargetRef().getName().equals(podName))
				.map(s -> (ServiceInstance) new KubernetesServiceInstance(serviceName,
					s.getAddresses().stream().findFirst().orElseThrow(IllegalStateException::new),
					s.getPorts().stream().findFirst().orElseThrow(IllegalStateException::new),
					endpoints.getMetadata().getAnnotations(), false))
				.findFirst().orElse(defaultInstance);
		} catch (Throwable ignored) { }

		return defaultInstance;
	}

	@Override
	public List<ServiceInstance> getInstances(String serviceId) {
		Assert.notNull(serviceId, "[Assertion failed] - the object argument must be null");
		Endpoints endpoints = getServiceEndpoints(serviceId);

		return endpoints
			.getSubsets()
			.stream()
			.flatMap(s -> s.getAddresses().stream().map(
				a -> (ServiceInstance) new KubernetesServiceInstance(serviceId, a,
					s.getPorts().stream().findFirst().orElseThrow(IllegalStateException::new),
					endpoints.getMetadata().getAnnotations(), false)))
			.collect(Collectors.toList());
	}

	@Override
	public List<String> getServices() {
		return client.services().list()
			.getItems()
			.stream().map(s -> s.getMetadata().getName())
			.collect(Collectors.toList());
	}

	private Endpoints getServiceEndpoints(String serviceId) {
		EndpointsList list = client.endpoints()
			.withField(METADATA_FIELD_NAME, serviceId)
			.list();

		if (null != list && !CollectionUtils.isEmpty(list.getItems())) {
			return list.getItems().get(0);
		}

		return new Endpoints();
	}
}
