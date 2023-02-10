from __future__ import annotations
import sys
from typing import Any

from racetrack_client.log.logs import get_logger

if 'lifecycle' in sys.modules:
    from lifecycle.deployer.infra_target import InfrastructureTarget
    from deployer import KubernetesJobDeployer
    from monitor import KubernetesMonitor
    from logs_streamer import KubernetesLogsStreamer

logger = get_logger(__name__)


class Plugin:

    def infrastructure_targets(self) -> dict[str, Any]:
        """
        Infrastructure Targets (deployment targets) for Job provided by this plugin
        :return dict of infrastructure name -> an instance of lifecycle.deployer.infra_target.InfrastructureTarget
        """
        return {
            'kubernetes': InfrastructureTarget(
                job_deployer=KubernetesJobDeployer(self.plugin_dir),
                job_monitor=KubernetesMonitor(),
                logs_streamer=KubernetesLogsStreamer(),
            ),
        }
