"""Custom configuration file for Celery for Quorum queue compatibility."""

import fcntl
import json
import os

from celery import bootsteps
from celery.worker.autoscale import Autoscaler
from kombu.common import QoS

from netskope.common.utils import Logger

logger = Logger()


GLOBAL_MAX_CONCURRENCY = int(
    os.environ.get("WORKER_CONCURRENCY")
    or (int(os.environ.get("SYSTEM_CORE", os.cpu_count() * 1.25)) - 3)
)

WORKER_PRIORITIES = {
    "worker_medium": {
        "priority": 6,
        "min_concurrency": 0,
        "max_concurrency": GLOBAL_MAX_CONCURRENCY,
    },
    "worker_low": {
        "priority": 3,
        "min_concurrency": 0,
        "max_concurrency": GLOBAL_MAX_CONCURRENCY,
    },
}

SORTED_WORKERS = sorted(WORKER_PRIORITIES.items(), key=lambda item: item[1]["priority"])

WORKER_WAITING = "worker_waiting"
REDUCE_PROCESS = "reduce_process"


class NoChannelGlobalQoS(bootsteps.StartStopStep):
    """Disable global prefetch as Quorum queue doesn't support it."""

    requires = {"celery.worker.consumer.tasks:Tasks"}

    def start(self, c):
        """Start step."""
        qos_global = False

        c.connection.default_channel.basic_qos(0, c.initial_prefetch_count, qos_global)

        def set_prefetch_count(prefetch_count):
            return c.task_consumer.qos(
                prefetch_count=prefetch_count,
                apply_global=qos_global,
            )

        c.qos = QoS(set_prefetch_count, c.initial_prefetch_count)


class CustomScaler(Autoscaler):
    """Custom autoscaler for Workers."""

    def __init__(self, *args, **kwargs):
        """Initialize CustomScaler class."""
        super(CustomScaler, self).__init__(*args, **kwargs)
        self.worker_name = str(self.worker).split("@")[0]

    def _grow(self, n):
        logger.debug(f"Scaling up {n} processes for {self.worker}.")
        self.pool.grow(n)
        self.pool.maintain_pool()

    def _shrink(self, n):
        logger.debug(f"Scaling down {n} processes for {self.worker}.")
        try:
            self.pool.shrink(n)
            self.pool.maintain_pool()
            return True
        except ValueError:
            logger.debug(
                f"{self.worker}: Autoscaler won't scale down: all processes busy."
            )
        except Exception as exc:
            logger.warn(f"{self.worker}: Error occurred while scaling down: {exc}")
        return False

    def maybe_scale(self, req=None):
        """Scale the number of workers if needed."""

        def reduce_count(decrease):
            """Shrink one worker at a time to reduce the possibility of failures due to running workers."""
            while decrease > 0:
                if not self._shrink(1):
                    return
                # Shrink is successful, reduce the counts for running workers and instructions.
                running_data.update(
                    {self.worker_name: running_data[self.worker_name] - 1}
                )
                running_data[REDUCE_PROCESS][self.worker_name] -= 1
                if decrease == 1:
                    del running_data[REDUCE_PROCESS][self.worker_name]
                decrease -= 1

        def find_low_priority_details(increase):
            """Find if the low priority workers can be reduced for high priority tasks."""
            instructions = running_data.get(REDUCE_PROCESS, {})
            # Prepare instructions for reducing the concurrency of low priority workers.
            for worker, worker_data in SORTED_WORKERS:
                if self.worker_name == worker:
                    return increase, instructions
                if running_data[worker] > worker_data["min_concurrency"]:
                    remove_count = min(
                        running_data[worker] - worker_data["min_concurrency"], increase
                    )
                    instructions[worker] = max(
                        remove_count, instructions.get(worker, 0)
                    )
                    increase -= remove_count
                if not increase:
                    return increase, instructions
            return increase, instructions

        def find_and_adjust():
            """Increase/decrease the workers if possible.

            If global max concurrency is reached, check if low priority tasks has occupied the workers.
            """
            procs = self.processes
            cur = min(self.qty, self.max_concurrency)

            # Need to increase the workers.
            if cur > procs:
                # Global limit is not reached.
                if total_running < GLOBAL_MAX_CONCURRENCY:
                    increase = min(cur - procs, GLOBAL_MAX_CONCURRENCY - total_running)
                    self._grow(increase)
                    # Delete or reduce required count.
                    running_data.update(
                        {self.worker_name: running_data[self.worker_name] + increase}
                    )
                    if (
                        WORKER_WAITING in running_data
                        and self.worker_name in running_data[WORKER_WAITING]
                    ):
                        if increase >= running_data[WORKER_WAITING][self.worker_name]:
                            del running_data[WORKER_WAITING][self.worker_name]
                        else:
                            running_data[WORKER_WAITING][self.worker_name] -= increase
                # Global limit is reached. Check priorities.
                else:
                    remaining, running_data[REDUCE_PROCESS] = find_low_priority_details(
                        cur - procs
                    )
                    if remaining == cur - procs:
                        return
                    # Instructions are updated. Add required count
                    if WORKER_WAITING not in running_data:
                        running_data[WORKER_WAITING] = dict()
                    # Add whole waiting number here, even if instructions are created for less numbers.
                    running_data[WORKER_WAITING][self.worker_name] = cur - procs

            cur = max(self.qty, self.min_concurrency)
            if cur < procs:
                decrease = procs - cur
                self._shrink(decrease)
                running_data.update(
                    {self.worker_name: running_data[self.worker_name] - decrease}
                )
                # This shrink happens when workers are idle. Remove the required count.
                if (
                    WORKER_WAITING in running_data
                    and self.worker_name in running_data[WORKER_WAITING]
                ):
                    del running_data[WORKER_WAITING][self.worker_name]

        def is_higher_priority_waiting():
            if WORKER_WAITING not in running_data:
                return False
            for i in range(len(SORTED_WORKERS) - 1, -1, -1):
                if SORTED_WORKERS[i][0] == self.worker_name:
                    return False
                if SORTED_WORKERS[i][0] in running_data[WORKER_WAITING]:
                    return True
            return False

        if self.worker_name == "worker_high":
            super().maybe_scale(req)
        else:
            # Use file for Inter process communication.
            with open("/opt/.netskope_workers", "r+") as fp:
                fcntl.flock(fp, fcntl.LOCK_EX)
                try:
                    running_data = fp.read()
                    if running_data:
                        try:
                            running_data = json.loads(running_data)
                        except json.JSONDecodeError:
                            logger.warn(
                                f"{self.worker}: Incorrect worker state file format detected. "
                                "The file will be restored with the minimum count. "
                                "Temporarily, this might lead to concurrency exceeding its limit.",
                                details=running_data,
                            )
                            running_data = {
                                key: value["min_concurrency"]
                                for key, value in WORKER_PRIORITIES.items()
                            }
                    else:
                        running_data = {
                            key: value["min_concurrency"]
                            for key, value in WORKER_PRIORITIES.items()
                        }

                    running_data.update({self.worker_name: self.processes})
                    total_running = (
                        running_data["worker_medium"]
                        + running_data["worker_low"]
                    )
                    if running_data.get(REDUCE_PROCESS, {}).get(self.worker_name):
                        reduce_count(running_data[REDUCE_PROCESS][self.worker_name])
                    else:
                        if is_higher_priority_waiting():
                            return
                        find_and_adjust()

                except Exception as exc:
                    logger.warn(
                        f"{self.worker}: Error occurred while running autoscaler. {exc}"
                    )
                finally:
                    fp.seek(0)
                    fp.truncate(0)
                    fp.write(json.dumps(running_data))
                    fcntl.flock(fp, fcntl.LOCK_UN)
