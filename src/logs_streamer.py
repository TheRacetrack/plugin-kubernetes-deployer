import threading
from typing import Callable
from collections import defaultdict

from kubernetes import client
from kubernetes.client import V1Pod
from kubernetes.watch import Watch

from lifecycle.monitor.base import LogsStreamer
from racetrack_commons.deploy.resource import job_resource_name

from utils import get_job_pod_names, k8s_api_client, K8S_NAMESPACE, K8S_JOB_RESOURCE_LABEL


class KubernetesLogsStreamer(LogsStreamer):
    """Source of a Job logs retrieved from a Kubernetes pod"""

    def __init__(self):
        super().__init__()
        self.sessions: dict[str, list[Watch]] = defaultdict(list)

    def create_session(self, session_id: str, resource_properties: dict[str, str], on_next_line: Callable[[str, str], None]):
        """Start a session transmitting messages to a client."""
        job_name = resource_properties.get('job_name')
        job_version = resource_properties.get('job_version')
        tail = resource_properties.get('tail')
        resource_name = job_resource_name(job_name, job_version)

        k8s_client = k8s_api_client()
        core_api = client.CoreV1Api(k8s_client)
        ret = core_api.list_namespaced_pod(K8S_NAMESPACE,
                                           label_selector=f'{K8S_JOB_RESOURCE_LABEL}={resource_name}')
        pods: list[V1Pod] = ret.items
        pod_names = get_job_pod_names(pods)

        for pod_name in pod_names:
            watch = Watch()
            self.sessions[session_id].append(watch)

            def watch_output():
                for line in watch.stream(core_api.read_namespaced_pod_log, name=pod_name, namespace=K8S_NAMESPACE,
                                         container=resource_name, tail_lines=tail, follow=True):
                    on_next_line(session_id, line)

            threading.Thread(target=watch_output, args=(), daemon=True).start()

    def close_session(self, session_id: str):
        watches = self.sessions[session_id]
        for watch in watches:
            watch.stop()
        del self.sessions[session_id]
