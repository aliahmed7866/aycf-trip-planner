import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('registry',ROOT/'termux/merge-app-registry.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class RegistryTests(unittest.TestCase):
    def test_merge_preserves_custom_port_and_apps(self):
        with tempfile.TemporaryDirectory() as directory:
            dest=Path(directory)/'apps.json'
            dest.write_text(json.dumps({'apps':[{'id':'mediahub','name':'Media Hub','port':9090,'open_url':'http://127.0.0.1:9090','health_url':'http://127.0.0.1:9090/health'},{'id':'custom','name':'Custom'}]}))
            module.merge(ROOT/'termux/apps.json.example',dest)
            module.merge(ROOT/'termux/apps.json.example',dest)
            apps=json.loads(dest.read_text())['apps']
            self.assertEqual(len(apps),5)
            self.assertEqual(apps[0]['port'],9090)
            self.assertIn('install_command',apps[0])
            self.assertEqual(apps[1]['id'],'custom')

    def test_malformed_registry_is_not_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            dest=Path(directory)/'apps.json';dest.write_text('broken')
            with self.assertRaises(ValueError):module.merge(ROOT/'termux/apps.json.example',dest)
            self.assertEqual(dest.read_text(),'broken')
