from __future__ import annotations

import asyncio
from typing import Tuple, Dict, Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

from src.hevo.providers.edge.hooks.edge import EdgeAsyncHook


class EdgeTrigger(BaseTrigger):
    def __init(self, task_id: str, pipeline_id: int,
               poke_interval: float = 4.0,
               ) -> None:
        super().__init__()
        self.job_id = task_id
        self.pipeline_id = pipeline_id
        self.poke_interval = poke_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes EdgeTrigger arguments and classpath."""
        return (
            "hevo_provider_edge.triggers.EdgeTrigger",
            {
                "job_id": self.job_id,
                "pipeline_id": self.pipeline_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        try:
            hook = EdgeAsyncHook(self.pipeline_id)
            if self.job_id is None:
                self.job_id = hook.get_active_job_for_type_async(self.pipeline_id)
            while True:
                res = await hook.job_completed_successfully_async(
                    self.pipeline_id, self.job_id
                )
                if res == "success":
                    msg = "Pipeline %d synced under job id %s" % (
                        self.pipeline_id,
                        self.job_id,
                    )
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": msg,
                            "return_value": self.job_id,
                        }
                    )
                    return
                elif res == "pending":
                    self.log.info("Job is still in pending state...")
                    self.log.info("sleeping for %s seconds.", self.poke_interval)
                    await asyncio.sleep(self.poke_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": "error"})
                    return
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
