from typing import Any

from racetrack_client.log.logs import get_logger
from lifecycle.infrastructure.model import InfrastructureTarget

from deployer import KubernetesJobDeployer
from monitor import KubernetesMonitor
from logs_streamer import KubernetesLogsStreamer

logger = get_logger(__name__)


class Plugin:

    def infrastructure_targets(self) -> dict[str, Any]:
        """
        Infrastructure Targets (deployment targets) for Job provided by this plugin
        :return dict of infrastructure name -> an instance of InfrastructureTarget
        """
        return {
            'kubernetes': InfrastructureTarget(
                job_deployer=KubernetesJobDeployer(self.plugin_dir),
                job_monitor=KubernetesMonitor(),
                logs_streamer=KubernetesLogsStreamer(),
            ),
        }
