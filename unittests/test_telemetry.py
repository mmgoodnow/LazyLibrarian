#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the anaonymous telemetry collection

import unittesthelpers
import json
import pytest
import pytest_order

import lazylibrarian
from lazylibrarian import config, telemetry


class TelemetryTest(unittesthelpers.LLTestCase):
 
    # Initialisation code that needs to run only once
    @classmethod
    def setUpClass(cls) -> None:
        super().setDoAll(all=False)
        super().setConfigFile('./unittests/testdata/testconfig-nondefault.ini')
        return super().setUpClass()

    def _do_ids_match(self):
        t = telemetry.LazyTelemetry()
        loaded_id = lazylibrarian.CONFIG['SERVER_ID'] if 'SERVER_ID' in lazylibrarian.CONFIG else None
        id = t.ensure_server_id(lazylibrarian.CONFIG)
        self.assertIsNotNone(id)
        if loaded_id:
            self.assertEqual(id, loaded_id)
        return id

    def test_getTelemetryObject(self):
        t = telemetry.LazyTelemetry()
        self.assertIsNotNone(t, 'Telemetry object must exist')
        t2 = telemetry.LazyTelemetry()
        self.assertEqual(t, t2, 'Telemetry object not acting as singleton')

    def test_ensure_server_id_generation(self):
        saved_id = self._do_ids_match()
        # Pretend we don't have an ID to ensure generation works
        telemetry.LazyTelemetry().clear_id(lazylibrarian.CONFIG)

        new_id = self._do_ids_match()
        self.assertNotEqual(saved_id, new_id, 'ID generation does not work')
        self.assertEqual(len(saved_id), len(new_id), 'Expect constant length IDs')

        # Restore to known good state
        telemetry.LazyTelemetry().clear_id(lazylibrarian.CONFIG)
        lazylibrarian.CONFIG['SERVER_ID'] = saved_id
        check_id = self._do_ids_match()
        self.assertEqual(saved_id, check_id, 'Test logic is broken')

    def test_ensure_server_id_persistence(self):
        my_id = self._do_ids_match()

        # Check we can read the new ID and test again
        config.config_write('Telemetry')
        self._do_ids_match()

    def test_set_install_data(self):
        t = telemetry.LazyTelemetry()
        t.set_install_data(lazylibrarian.CONFIG)
        srv = t.get_server_telemetry()
        self.assertIsInstance(srv, dict)
        self.assertEqual(srv['id'], lazylibrarian.CONFIG['SERVER_ID'])
        self.assertIsInstance(srv['uptime_seconds'], int)

    def test_set_config_data(self):
        t = telemetry.LazyTelemetry()

        t.set_config_data(lazylibrarian.CONFIG)
        cfg = t.get_config_telemetry()

        self.assertIsInstance(cfg, dict)
        # Helpful to create new json_good data:
        # json_fromcfg = json.dumps(obj=cfg)
        # print(json_fromcfg) 
        json_good = json.loads("""
            {"switches": "EBOOK_TAB COMIC_TAB SERIES_TAB BOOK_IMG MAG_IMG COMIC_IMG AUTHOR_IMG API_ENABLED CALIBRE_USE_SERVER OPF_TAGS ", 
            "params": "IMP_CALIBREDB DOWNLOAD_DIR API_KEY ", 
            "BOOK_API": "OpenLibrary", 
            "NEWZNAB": 1, "TORZNAB": 0, "RSS": 0, "IRC": 0, "GEN": 0, "APPRISE": 0}
        """)
        self.assertEqual(cfg, json_good, "Config not as expected. Check that ini file has not changed")

    def test_record_usage_data(self):
        t = telemetry.LazyTelemetry()
        t.record_usage_data("API/getHelp")
        t.record_usage_data("web/test")
        t.record_usage_data("API/getHelp")
        t.record_usage_data("Download/NZB")

        usg = t.get_usage_telemetry()
        # TODO/AM: As for cfg, compare dicts for completeness' sake
        self.assertEqual(usg["API/getHelp"], 2)
        self.assertEqual(usg["web/test"], 1)
        jsoncfg = json.dumps(obj=usg)

    @pytest.mark.order(after="test_ensure_server_id_generation")
    @pytest.mark.order(after="test_ensure_server_id_persistence")
    @pytest.mark.order(after="test_set_config_data")
    @pytest.mark.order(after="test_record_usage_data")
    def test_get_json(self):
        t = telemetry.LazyTelemetry()
        t.set_install_data(lazylibrarian.CONFIG)
        # Uncomment to generate new test data:
        # datastr = t.get_json(pretty=True)
        # print(datastr)

        f = open('./unittests/testdata/telemetry-sample.json')
        try:
            loadedjson = json.load(f)
        finally:
            f.close()

        self.assertEqual(t._data, loadedjson, "The JSON telemetry data is not as expected")

    def test_submit_data(self):
        t = telemetry.LazyTelemetry()
        pass

