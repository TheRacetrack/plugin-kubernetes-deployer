import json
import backoff

from typing import Callable, Iterable

from kubernetes import client
from kubernetes.client import V1ObjectMeta, V1PodStatus, ApiException

from lifecycle.config import Config
from lifecycle.monitor.base import JobMonitor
from lifecycle.monitor.health import check_until_job_is_operational, quick_check_job_condition
from lifecycle.monitor.metric_parser import read_last_call_timestamp_metric, scrape_metrics
from racetrack_client.log.context_error import wrap_context
from racetrack_client.log.exception import short_exception_details
from racetrack_client.utils.shell import CommandError, shell_output
from racetrack_client.utils.time import datetime_to_timestamp
from racetrack_commons.deploy.resource import job_resource_name
from racetrack_commons.entities.dto import JobDto, JobStatus
from racetrack_client.log.logs import get_logger

from utils import get_recent_job_pod, k8s_api_client, K8S_JOB_NAME_LABEL, K8S_JOB_VERSION_LABEL, \
    K8S_NAMESPACE, K8S_JOB_RESOURCE_LABEL, get_job_deployments, get_job_pods

logger = get_logger(__name__)


class KubernetesMonitor(JobMonitor):
    """Discovers Job resources in a k8s cluster and monitors their condition"""

    def __init__(self) -> None:
        self.infrastructure_name = 'kubernetes'

    def list_jobs(self, config: Config) -> Iterable[JobDto]:
        # Ideally these should be in __init__, but that breaks test_bootstrap.py
        k8s_client = k8s_api_client()
        core_api = client.CoreV1Api(k8s_client)
        apps_api = client.AppsV1Api(k8s_client)

        with wrap_context('listing Kubernetes API'):
            deployments = get_job_deployments(apps_api)
            pods_by_job = get_job_pods(core_api)

        for resource_name, deployment in deployments.items():
            pods = pods_by_job.get(resource_name)
            if pods is None or len(pods) == 0:
                continue

            recent_pod = get_recent_job_pod(pods)
            metadata: V1ObjectMeta = recent_pod.metadata
            job_name = metadata.labels.get(K8S_JOB_NAME_LABEL)
            job_version = metadata.labels.get(K8S_JOB_VERSION_LABEL)
            if not (job_name and job_version):
                continue

            start_timestamp = datetime_to_timestamp(recent_pod.metadata.creation_timestamp)
            internal_name = f'{resource_name}.{K8S_NAMESPACE}.svc:7000'

            replica_internal_names: list[str] = []
            for pod in pods:
                pod_status: V1PodStatus = pod.status
                if pod_status.pod_ip:
                    pod_ip_dns: str = pod_status.pod_ip.replace('.', '-')
                    replica_internal_names.append(
                        f'{pod_ip_dns}.{resource_name}.{K8S_NAMESPACE}.svc:7000'
                    )
            replica_internal_names.sort()

            job = JobDto(
                name=job_name,
                version=job_version,
                status=JobStatus.RUNNING.value,
                create_time=start_timestamp,
                update_time=start_timestamp,
                manifest=None,
                internal_name=internal_name,
                error=None,
                infrastructure_target=self.infrastructure_name,
                replica_internal_names=replica_internal_names,
            )
            try:
                job_url = self._get_internal_job_url(job)
                quick_check_job_condition(job_url)
                job_metrics = scrape_metrics(f'{job_url}/metrics')
                job.last_call_time = read_last_call_timestamp_metric(job_metrics)
            except Exception as e:
                error_details = short_exception_details(e)
                job.error = error_details
                job.status = JobStatus.ERROR.value
                logger.warning(f'Job {job} is in bad condition: {error_details}')
            yield job

    def check_job_condition(self,
                            job: JobDto,
                            deployment_timestamp: int = 0,
                            on_job_alive: Callable = None,
                            logs_on_error: bool = True,
                            ):
        try:
            self.check_k8s_events_for_errors(job_resource_name(job.name, job.version))
            check_until_job_is_operational(self._get_internal_job_url(job),
                                           deployment_timestamp, on_job_alive)
        except Exception as e:
            if logs_on_error:
                try:
                    logs = self.read_recent_logs(job)
                except (AssertionError, ApiException, CommandError):
                    raise RuntimeError(str(e)) from e
                raise RuntimeError(f'{e}\nJob logs:\n{logs}') from e
            else:
                raise RuntimeError(str(e)) from e

    def read_recent_logs(self, job: JobDto, tail: int = 20) -> str:
        resource_name = job_resource_name(job.name, job.version)
        return shell_output(f'kubectl logs'
                            f' --selector {K8S_JOB_RESOURCE_LABEL}={resource_name}'
                            f' -n {K8S_NAMESPACE}'
                            f' --tail={tail}'
                            f' --container={resource_name}')

    def _get_internal_job_url(self, job: JobDto) -> str:
        return f'http://{job.internal_name}'

    @backoff.on_exception(backoff.fibo, RuntimeError, max_value=2, max_time=10, jitter=None, logger=None)
    def check_k8s_events_for_errors(self, resource_name: str):
        # First, query relevant Deployment events based on resource_name, e.g. "job-adder-v-0-0-2"
        base_cmd = (f'kubectl get events '
                    f'--namespace {K8S_NAMESPACE} '
                    f'--sort-by=\'.metadata.creationTimestamp\' '
                    f'-o json' )
        deployment_selectors = f'--field-selector involvedObject.kind=Deployment,involvedObject.name={resource_name}'
        deployment_query = json.loads(shell_output(f'{base_cmd} {deployment_selectors}'))

        # From the deployment query, we extract the relevant ReplicaSet names
        replicaset_names = self.get_replicaset_names_from_deployment_query(deployment_query, resource_name)

        # Query for all the ReplicaSets
        replicaset_queries = [
            json.loads(shell_output(f'{base_cmd} --field-selector involvedObject.kind=ReplicaSet,involvedObject.name={name}'))
            for name in replicaset_names
        ]

        # From the ReplicaSet queries, we get the relevant Pod names
        pod_names = self.get_pod_names_from_replicaset_queries(replicaset_queries, resource_name)

        # Query for all the Pods
        pod_queries = [
            json.loads(shell_output(f'{base_cmd} --field-selector involvedObject.kind=Pod,involvedObject.name={pod_name}'))
            for pod_name in pod_names
        ]

        # Then, we check the pod queries for errors.
        errors_we_dont_want_in_messages = ['Insufficient cpu']
        errors_we_dont_want_in_reasons = ['FailedScheduling']
        for pod_query in pod_queries:
            for event in pod_query.get('items', []):
                for error in errors_we_dont_want_in_messages:
                    if error in event['message'] and event['type'] == 'Warning':
                        raise(RuntimeError(f'Kubernets event error: {str(event)}'))
                for error in errors_we_dont_want_in_reasons:
                    if error in event['reason'] and event['type'] == 'Warning':
                        raise(RuntimeError(f'Kubernets event error: {str(event)}'))


    def get_replicaset_names_from_deployment_query(self, deployment_query, resource_name):
        # Assumes events in the query are sorted from oldest to newest, so the latest one should be last element
        # We only look at the latest event from the query, hence the [-1]
        deployment_messages = [event['message'] for event in [deployment_query.get('items', [])[-1]]]
        replicaset_names = [
            self.get_first_word_starting_with(message, resource_name)
            for message in deployment_messages
        ]

        return set(replicaset_names)

    def get_pod_names_from_replicaset_queries(self, replicaset_queries, resource_name):
        pod_names = [
            self.get_first_word_starting_with(message, resource_name)
            for replicaset_query in replicaset_queries
            for message in [event['message'] for event in replicaset_query.get('items', [])]
        ]
        return pod_names

    def get_first_word_starting_with(self, string_of_words, starting_with):
        return next(word for word in string_of_words.split() if word.startswith(f'{starting_with}-'))
