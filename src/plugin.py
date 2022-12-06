from __future__ import annotations
from typing import Any

from racetrack_client.log.logs import get_logger

logger = get_logger(__name__)

try:
    from lifecycle.deployer.infra_target import InfrastructureTarget
    from deployer import KubernetesFatmanDeployer
    from monitor import KubernetesMonitor
    from logs_streamer import KubernetesLogsStreamer
except ModuleNotFoundError:
    logger.debug('Skipping Lifecycle\'s imports')


class Plugin:

    def infrastructure_targets(self) -> dict[str, Any]:
        """
        Infrastracture Targets (deployment targets) for Fatmen provided by this plugin
        :return dict of infrastructure name -> an instance of lifecycle.deployer.infra_target.InfrastructureTarget
        """
        return {
            'kubernetes': InfrastructureTarget(
                'kubernetes',
                fatman_deployer=KubernetesFatmanDeployer(),
                fatman_monitor=KubernetesMonitor(),
                logs_streamer=KubernetesLogsStreamer(),
            ),
        }
