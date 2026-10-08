import sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import dataforge_pipeline as pipeline
class PipelineTests(unittest.TestCase):
    def test_incremental_and_quality(self):
        with tempfile.TemporaryDirectory() as d:
            original=pipeline.DATA,pipeline.OUT,pipeline.DB
            try:
                pipeline.DATA=Path(d)/'data';pipeline.OUT=Path(d)/'outputs';pipeline.DB=pipeline.DATA/'warehouse.sqlite'
                pipeline.generate(1000)
                first=pipeline.run();self.assertGreater(first['inserted'],0);self.assertGreater(first['rejected'],0)
                self.assertEqual(first['fact_orders'],first['inserted'])
                second=pipeline.run();self.assertEqual(second['inserted'],0);self.assertEqual(second['fact_orders'],first['fact_orders'])
                self.assertTrue(all(pipeline.check().values()))
            finally:pipeline.DATA,pipeline.OUT,pipeline.DB=original
if __name__=='__main__':unittest.main()
