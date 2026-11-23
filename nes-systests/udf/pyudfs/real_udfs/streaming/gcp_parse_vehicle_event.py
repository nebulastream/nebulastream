# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["{\"vehicle_id\": \"V-1001\", \"timestamp\": \"2024-05-14T09:30:00Z\", \"temperature\": 92, \"rpm\": 2450, \"vibration\": 0.31, \"fuel_level\": 64, \"mileage\": 48211}"], ["{\"vehicle_id\": \"V-1002\", \"timestamp\": \"2024-05-14T11:30:05.250+02:00\", \"temperature\": 101, \"rpm\": 3900, \"vibration\": 1.75, \"fuel_level\": 12, \"mileage\": 120544}"], ["{\"vehicle_id\": 1003, \"timestamp\": \"2024-05-14T09:31:10\", \"temperature\": \"88\", \"rpm\": 800, \"vibration\": \"0.05\", \"fuel_level\": 99, \"mileage\": 15}"], ["{\"vehicle_id\": \"V-1004\", \"timestamp\": \"2024-05-14 09:32:00Z\", \"temperature\": 95, \"rpm\": 2100}"], ["{\"vehicle_id\": \"V-1005\", \"timestamp\": \"2024-05-14T09:33:00Z\", \"temperature\": 90, \"rpm\": \"n/a\", \"vibration\": 0.4, \"fuel_level\": 50, \"mileage\": 70000}"], ["{\"vehicle_id\": \"V-1006\", \"timestamp\": \"2024-05-14T09:34:00Z\", \"temperature\": 9"]]
# real-udf: origin=streaming id=gcp_parse_vehicle_event requires=apache-beam wrapped=1 ported=0
# Wrapper: case_entry calls ParseVehicleEventDoFn.process on one instance and returns the parsed event as JSON text (timestamp in ISO format), or "" when the DoFn yields nothing (parse error). No sample row lacks a timestamp, so the datetime.now() fallback is not taken.
# Imports: the original module's imports.

import datetime
import json
import logging
from typing import Any, Dict, Iterator, NamedTuple, Union

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms.window import TimestampedValue

# Source: https://github.com/GoogleCloudPlatform/dataflow-solution-guides/blob/9e087e871bb4c860e317b661ac5847ecccbcda7d/pipelines/iot_analytics/iot_analytics_pipeline/parse_timestamp.py#L26-L95
# Repository: https://github.com/GoogleCloudPlatform/dataflow-solution-guides (commit 9e087e871bb4c860e317b661ac5847ecccbcda7d)
# License: Apache-2.0
# System: Apache Beam
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def parse_timestamp(timestamp_str: str) -> datetime.datetime:
  """Parses RFC3339 or ISO8601 timestamp string safely."""
  if not timestamp_str:
    return datetime.datetime.now(datetime.timezone.utc)
  try:
    # Handle 'Z' suffix for Python fromisoformat
    normalized = timestamp_str.replace("Z", "+00:00")
    dt = datetime.datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
      dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt
  except (ValueError, TypeError):
    logging.warning("Could not parse timestamp '%s', using UTC now",
                    timestamp_str)
    return datetime.datetime.now(datetime.timezone.utc)


class VehicleStateEvent(NamedTuple):
  """Container for vehicle telemetry event."""
  vehicle_id: str
  timestamp: datetime.datetime
  temperature: int
  rpm: int
  vibration: float
  fuel_level: int
  mileage: int

  @staticmethod
  def convert_json_to_vehicleobj(
      input_json: Dict[str, Any]) -> TimestampedValue:
    """Converts a parsed JSON dictionary into a TimestampedValue of VehicleStateEvent."""
    dt_object = parse_timestamp(input_json.get("timestamp", ""))
    event = VehicleStateEvent(
        vehicle_id=str(input_json.get("vehicle_id", "")),
        timestamp=dt_object,
        temperature=int(input_json.get("temperature", 0)),
        rpm=int(input_json.get("rpm", 0)),
        vibration=float(input_json.get("vibration", 0.0)),
        fuel_level=int(input_json.get("fuel_level", 0)),
        mileage=int(input_json.get("mileage", 0)))
    return TimestampedValue(event, dt_object.timestamp())


class ParseVehicleEventDoFn(beam.DoFn):
  """Parses raw Pub/Sub bytes or JSON string into VehicleStateEvent with metrics."""

  def __init__(self):
    super().__init__()
    self.processed_counter = Metrics.counter(self.__class__, "events_parsed")
    self.parse_error_counter = Metrics.counter(self.__class__,
                                               "events_parse_errors")

  def process(
      self, element: Union[bytes, str,
                           Dict[str, Any]]) -> Iterator[TimestampedValue]:
    try:
      if isinstance(element, bytes):
        payload = json.loads(element.decode("utf-8"))
      elif isinstance(element, str):
        payload = json.loads(element)
      elif isinstance(element, dict):
        payload = element
      else:
        raise ValueError(f"Unsupported element type: {type(element)}")

      self.processed_counter.inc()
      yield VehicleStateEvent.convert_json_to_vehicleobj(payload)
    except Exception as e:  # pylint: disable=broad-exception-caught
      self.parse_error_counter.inc()
      logging.error("Failed to parse event element '%s': %s", element, e)


_parse_vehicle_event_fn = ParseVehicleEventDoFn()


def case_entry(element):
    values = list(_parse_vehicle_event_fn.process(element))
    if not values:
        return ""
    fields = values[0].value._asdict()
    fields["timestamp"] = fields["timestamp"].isoformat()
    return json.dumps(fields, sort_keys=True)
