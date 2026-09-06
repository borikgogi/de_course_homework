"""GHArchiveSensor — ВАШ custom sensor. Специфікація: ../../SPEC.md → «Sensor».

Сенсор чекає, поки годинний файл GitHub Archive за logical date стане доступним,
і лише тоді пропускає DAG далі.

Підказки:
  * успадкуйте `airflow.sensors.base.BaseSensorOperator`;
  * у __init__ прийміть параметр `hour` (година доби, яку перевіряємо);
  * реалізуйте `poke(self, context) -> bool`: візьміть дату з context["ds"],
    зберіть URL https://data.gharchive.org/<ds>-<hour>.json.gz і зробіть HTTP HEAD —
    поверніть True на 200, інакше False (або при винятку);
  * у DAG додайте сенсор першою задачею з timeout=600, poke_interval=60,
    mode="reschedule".
"""

from __future__ import annotations
import urllib.request
from airflow.sensors.base import BaseSensorOperator


class GHArchiveSensor(BaseSensorOperator):
    """
    Checks if GitHub Archive file is available for the logical date and hour via HTTP HEAD.
    """

    template_fields = ("hour",)

    def __init__(self, hour: int = 14, **kwargs):
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        ds = context["ds"]
        url = f"https://data.gharchive.org/{ds}-{self.hour}.json.gz"
        self.log.info("Checking availability for: %s", url)

        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Airflow-GHArchive-Sensor/1.0"},
                method="HEAD",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    self.log.info("Archive found for %s (%s:00 UTC)", ds, self.hour)
                    return True
                return False
        except Exception as e:
            self.log.info("Archive not ready at %s (error: %s)", url, e)
            return False