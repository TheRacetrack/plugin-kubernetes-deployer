from typing import Callable, Iterable

from kubernetes import client
from kubernetes.client import V1ObjectMeta, ApiException

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

from utils import k8s_api_client, K8S_JOB_NAME_LABEL, K8S_JOB_VERSION_LABEL, \
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
            pods = get_job_pods(core_api)

        for resource_name, deployment in deployments.items():
            pod = pods.get(resource_name)
            if pod is None:
                continue

            metadata: V1ObjectMeta = pod.metadata
            job_name = metadata.labels.get(K8S_JOB_NAME_LABEL)
            job_version = metadata.labels.get(K8S_JOB_VERSION_LABEL)
            if not (job_name and job_version):
                continue

            start_timestamp = datetime_to_timestamp(pod.metadata.creation_timestamp)
            internal_name = f'{resource_name}.{K8S_NAMESPACE}.svc:7000'
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
            check_until_job_is_operational(self._get_internal_job_url(job),
                                              deployment_timestamp, on_job_alive)
        except Exception as e:
            if logs_on_error:
                try:
                    logs = self.read_recent_logs(job)
                except (AssertionError, ApiException, CommandError):
                    raise RuntimeError(str(e))
                raise RuntimeError(f'{e}\nJob logs:\n{logs}')
            else:
                raise RuntimeError(str(e))

    def read_recent_logs(self, job: JobDto, tail: int = 20) -> str:
        resource_name = job_resource_name(job.name, job.version)
        return shell_output(f'kubectl logs'
                            f' --selector {K8S_JOB_RESOURCE_LABEL}={resource_name}'
                            f' -n {K8S_NAMESPACE}'
                            f' --tail={tail}'
                            f' --container={resource_name}')

    def _get_internal_job_url(self, job: JobDto) -> str:
        return f'http://{job.internal_name}'
