# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224"], ["user16_AmaranthKoala,AmaranthKoala,18,1447719060000,2015-11-16 16:11:03.921"], ["user0_MagentaKangaroo,MagentaKangaroo,3,1447719060000,2015-11-16 16:11:03.955"], ["\"user7_Antique,Brass\",AntiqueBrassPlatypus,7,1447886400000,2015-11-18 14:40:00.000"], ["user19_BisqueBilby,BisqueBilby,xx,1447955630000,2015-11-19 09:53:53.444"], ["user5_BananaWallaby,BananaWallaby"]]
# real-udf: origin=streaming id=beam_parse_game_event requires=apache-beam wrapped=1 ported=0
# Wrapper: case_entry calls ParseGameEventFn.process on one instance and returns the dicts it yields as JSON text.
# Imports: the original module's imports that this code uses.

import csv
import json
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

# Source: https://github.com/apache/beam/blob/7f2fd8b6b080ad65ee49a2c75045fc34bec7685c/sdks/python/apache_beam/examples/complete/game/user_score.py#L88-L115
# Repository: https://github.com/apache/beam (commit 7f2fd8b6b080ad65ee49a2c75045fc34bec7685c)
# License: Apache-2.0
# System: Apache Beam
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

class ParseGameEventFn(beam.DoFn):
  """Parses the raw game event info into a Python dictionary.

  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time

  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

  The human-readable time string is not used here.
  """
  def __init__(self):
    super().__init__()
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, elem):
    try:
      row = list(csv.reader([elem]))[0]
      yield {
          'user': row[0],
          'team': row[1],
          'score': int(row[2]),
          'timestamp': int(row[3]) / 1000.0,
      }
    except:  # pylint: disable=bare-except
      # Log and count parse errors
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)


_parse_game_event_fn = ParseGameEventFn()


def case_entry(elem):
    return json.dumps(list(_parse_game_event_fn.process(elem)), sort_keys=True)
