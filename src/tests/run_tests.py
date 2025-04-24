import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tests.test_node_change_handler import TestNodeChangeHandler
from tests.test_database import TestDatabase
from tests.test_plc_collector import TestPLCCollector


def run_tests():
    test_suite = unittest.TestSuite()
    
    test_suite.addTest(unittest.makeSuite(TestNodeChangeHandler))
    test_suite.addTest(unittest.makeSuite(TestDatabase))
    test_suite.addTest(unittest.makeSuite(TestPLCCollector))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result

if __name__ == "__main__":
    result = run_tests()
    if not result.wasSuccessful():
        sys.exit(1)