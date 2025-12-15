#  This file is part of Lazylibrarian.
#
# Purpose:
#   Test functions in postprocess_refactor.py

import logging
import os
import tempfile
import time
from pathlib import Path
from unittest import mock

from lazylibrarian.postprocess_refactor import (
    BookState,
    _normalize_title,
    _tokenize_file,
    _is_valid_media_file,
    _count_zipfiles_in_directory,
    _find_valid_file_in_directory,
    _calculate_fuzzy_match,
    _validate_candidate_directory,
    _should_delete_processed_files,
    _calculate_download_age,
    _handle_seeding_status,
    _handle_snatched_timeout,
    _handle_aborted_download,
    _check_and_schedule_next_run,
)
from lazylibrarian.postprocess_metadata import (
    BookType,
    prepare_book_metadata,
    prepare_magazine_metadata,
    prepare_comic_metadata,
)

from lazylibrarian.config2 import CONFIG
from lazylibrarian.database import DBConnection
from unittests.unittesthelpers import LLTestCaseWithStartup


class BookStateTest(LLTestCaseWithStartup):
    """Test the BookState dataclass"""

    def test_from_db_row_ebook(self):
        """Test creating BookState from a database row for eBook"""
        book_row = {
            "BookID": "12345",
            "NZBtitle": "Test_Book_Title",
            "NZBmode": "nzb",
            "AuxInfo": "",
            "Completed": str(int(time.time())),
            "Source": "SABnzbd",
            "DownloadID": "download123",
            "NZBurl": "http://example.com/book.nzb",
            "NZBprov": "TestProvider",
            "Status": "Snatched",
            "NZBdate": "2025-12-06 10:00:00",
        }

        book_state = BookState.from_db_row(book_row, CONFIG)

        self.assertEqual(book_state.book_id, "12345")
        self.assertEqual(book_state.download_title, "Test Book Title")
        self.assertEqual(book_state.mode_type, "nzb")
        self.assertEqual(book_state.source, "SABnzbd")
        self.assertEqual(book_state.download_id, "download123")
        self.assertEqual(book_state.download_url, "http://example.com/book.nzb")
        self.assertEqual(book_state.download_provider, "TestProvider")
        self.assertEqual(book_state.status, "Snatched")
        self.assertFalse(book_state.is_torrent())
        self.assertTrue(book_state.is_completed())

    def test_from_db_row_torrent(self):
        """Test creating BookState from torrent download"""
        book_row = {
            "BookID": "67890",
            "NZBtitle": "Audiobook Title",
            "NZBmode": "torrent",
            "AuxInfo": "",
            "Completed": str(int(time.time() - 100)),
            "Source": "Deluge",
            "DownloadID": "abc123",
            "NZBurl": "magnet:?xt=urn:btih:...",
            "NZBprov": "TorrentProvider",
            "Status": "Snatched",
            "NZBdate": "",
        }

        book_state = BookState.from_db_row(book_row, CONFIG)

        self.assertEqual(book_state.book_id, "67890")
        self.assertEqual(book_state.mode_type, "torrent")
        self.assertTrue(book_state.is_torrent())
        self.assertTrue(book_state.is_completed())

    def test_is_completed(self):
        """Test completion status checking"""
        book_state = BookState(
            book_id="123", download_title="Test", aux_type="eBook", completed_at=0
        )
        self.assertFalse(book_state.is_completed())

        book_state.completed_at = int(time.time())
        self.assertTrue(book_state.is_completed())

    def test_seconds_since_completion(self):
        """Test elapsed time calculation"""
        book_state = BookState(
            book_id="123",
            download_title="Test",
            aux_type="eBook",
            completed_at=int(time.time() - 50),
        )

        elapsed = book_state.seconds_since_completion()
        self.assertGreaterEqual(elapsed, 50)
        self.assertLess(elapsed, 55)  # Should be ~50 seconds

    def test_should_delay_processing(self):
        """Test processing delay logic"""
        book_state = BookState(
            book_id="123",
            download_title="Test",
            aux_type="eBook",
            completed_at=int(time.time() - 30),
        )

        # Should delay if completed less than 60 seconds ago
        should_delay, elapsed = book_state.should_delay_processing(60)
        self.assertTrue(should_delay)
        self.assertGreaterEqual(elapsed, 30)

        # Should not delay if completed more than 60 seconds ago
        book_state.completed_at = int(time.time() - 100)
        should_delay, elapsed = book_state.should_delay_processing(60)
        self.assertFalse(should_delay)
        self.assertGreaterEqual(elapsed, 100)

    def test_update_candidate(self):
        """Test candidate pointer updates"""
        book_state = BookState(book_id="123", download_title="Test", aux_type="eBook")

        self.assertIsNone(book_state.candidate_ptr)
        self.assertFalse(book_state.has_candidate())

        book_state.update_candidate("/path/to/file.epub")
        self.assertEqual(book_state.candidate_ptr, "/path/to/file.epub")
        self.assertTrue(book_state.has_candidate())

    def test_mark_skipped(self):
        """Test skip status tracking"""
        book_state = BookState(book_id="123", download_title="Test", aux_type="eBook")

        self.assertFalse(book_state.is_skipped())

        book_state.mark_skipped("No valid files found")
        self.assertTrue(book_state.is_skipped())
        self.assertEqual(book_state.skipped_reason, "No valid files found")

    def test_is_book_type(self):
        """Test book type checking"""
        ebook_state = BookState(book_id="123", download_title="Test", aux_type="eBook")
        self.assertTrue(ebook_state.is_book())
        self.assertFalse(ebook_state.is_magazine())

        audio_state = BookState(
            book_id="123", download_title="Test", aux_type="AudioBook"
        )
        self.assertTrue(audio_state.is_book())

        mag_state = BookState(book_id="123", download_title="Test", aux_type="Magazine")
        self.assertFalse(mag_state.is_book())
        self.assertTrue(mag_state.is_magazine())

    def test_get_book_type_str(self):
        """Test booktype string conversion"""
        test_cases = [
            ("ebook", BookType.EBOOK.value),
            ("eBook", BookType.EBOOK.value),
            ("e Book", BookType.EBOOK.value),
            ("e-book", BookType.EBOOK.value),
            ("e_book", BookType.EBOOK.value),
            ("audiobook", BookType.AUDIOBOOK.value),
            ("AudioBook", BookType.AUDIOBOOK.value),
            ("Audio Book", BookType.AUDIOBOOK.value),
            ("audio-book", BookType.AUDIOBOOK.value),
            ("audio Book", BookType.AUDIOBOOK.value),
            ("AudioBook", BookType.AUDIOBOOK.value),
            ("magazine", BookType.MAGAZINE.value),
            ("Magazine", BookType.MAGAZINE.value),
            (" MaGazine ", BookType.MAGAZINE.value),
            ("comic", BookType.COMIC.value),
            (" comic ", BookType.COMIC.value),
            ("Comic", BookType.COMIC.value),
            ("CoMic", BookType.COMIC.value),
        ]

        for aux_type, expected in test_cases:
            book_state = BookState(
                book_id="123", download_title="Test", aux_type=aux_type
            )
            self.assertEqual(book_state.get_book_type_str(), expected)

    def test_can_delete_from_client(self):
        """Test download client deletion validation"""
        # No source
        book_state = BookState(book_id="123", download_title="Test", aux_type="eBook")
        self.assertFalse(book_state.can_delete_from_client())

        # Has source but no download_id
        book_state.source = "SABnzbd"
        self.assertFalse(book_state.can_delete_from_client())

        # Has download_id = "unknown"
        book_state.download_id = "unknown"
        self.assertFalse(book_state.can_delete_from_client())

        # DIRECT source (can't delete)
        book_state.source = "DIRECT"
        book_state.download_id = "123"
        self.assertFalse(book_state.can_delete_from_client())

        # Valid for deletion
        book_state.source = "SABnzbd"
        book_state.download_id = "valid_id"
        self.assertTrue(book_state.can_delete_from_client())

    def test_status_checks(self):
        """Test status checking methods"""
        book_state = BookState(
            book_id="123", download_title="Test", aux_type="eBook", status="Snatched"
        )
        self.assertTrue(book_state.is_snatched())
        self.assertFalse(book_state.is_seeding())
        self.assertFalse(book_state.is_aborted())

        book_state.status = "Seeding"
        self.assertFalse(book_state.is_snatched())
        self.assertTrue(book_state.is_seeding())

        book_state.status = "Aborted"
        self.assertTrue(book_state.is_aborted())

    def test_enrich_with_download_info_ebook(self):
        """Test enrichment with download folder and book title for ebook"""
        from unittest import mock
        from lazylibrarian.database import DBConnection

        db = DBConnection()
        try:
            # Insert test book
            db.action(
                "INSERT OR REPLACE INTO authors (AuthorID, AuthorName) VALUES (?, ?)",
                ("author1", "Test Author"),
            )
            db.action(
                "INSERT OR REPLACE INTO books (BookID, AuthorID, BookName) VALUES (?, ?, ?)",
                ("book123", "author1", "Test Book"),
            )

            book_state = BookState(
                book_id="book123",
                download_title="Test Download",
                aux_type="eBook",
                source="QBITTORRENT",
                download_id="dl123",
            )

            # Mock get_download_folder
            with mock.patch(
                "lazylibrarian.download_client.get_download_folder"
            ) as mock_get_folder:
                mock_get_folder.return_value = "/downloads/test_folder"

                book_state.enrich_with_download_info(db)

            # Verify enrichment
            self.assertEqual(book_state.download_folder, "/downloads/test_folder")
            self.assertIn("test author", book_state.book_title.lower())
            self.assertIn("test book", book_state.book_title.lower())
        finally:
            db.close()

    def test_enrich_with_download_info_no_folder(self):
        """Test enrichment when client doesn't provide download folder"""
        from unittest import mock
        from lazylibrarian.database import DBConnection

        db = DBConnection()
        try:
            db.action(
                "INSERT OR REPLACE INTO authors (AuthorID, AuthorName) VALUES (?, ?)",
                ("author1", "Test Author"),
            )
            db.action(
                "INSERT OR REPLACE INTO books (BookID, AuthorID, BookName) VALUES (?, ?, ?)",
                ("book123", "author1", "Test Book"),
            )

            book_state = BookState(
                book_id="book123",
                download_title="Test Download",
                aux_type="eBook",
                source="SABNZBD",
                download_id="nzb123",
            )

            # Mock get_download_folder returning None
            with mock.patch(
                "lazylibrarian.download_client.get_download_folder"
            ) as mock_get_folder:
                mock_get_folder.return_value = None

                book_state.enrich_with_download_info(db)

            # Should still get book_title but no download_folder
            self.assertEqual(book_state.download_folder, "")
            self.assertIn("test author", book_state.book_title.lower())
        finally:
            db.close()

    def test_enrich_with_download_info_magazine(self):
        """Test enrichment for magazine gets title"""
        from lazylibrarian.database import DBConnection

        db = DBConnection()
        try:
            db.action(
                "INSERT OR REPLACE INTO magazines (Title) VALUES (?)",
                ("Test Magazine",),
            )

            book_state = BookState(
                book_id="Test Magazine",
                download_title="Test Magazine Download",
                aux_type="Magazine",
                source="",
                download_id="",
            )

            book_state.enrich_with_download_info(db)

            # Should get magazine title
            self.assertIn("test magazine", book_state.book_title.lower())
            self.assertEqual(book_state.download_folder, "")
        finally:
            db.close()


class PostprocessHelperTest(LLTestCaseWithStartup):
    """Test postprocess helper functions"""

    def test_normalize_title(self):
        """Test title normalization"""
        test_cases = [
            ("Test_Book_Title", "Test Book Title"),
            ("Book LL.(12345)", "Book"),
            ("Über_Book_Name", "Uber Book Name"),
            ("  Spaces   Everywhere  ", "Spaces   Everywhere"),
            ("Book/With:Special*Chars", "Book_WithSpecialChars"),
        ]

        for input_title, expected in test_cases:
            result = _normalize_title(input_title)
            self.assertEqual(result, expected)

    def test_enforce_str(self):
        """Test enforce_str wrapper"""
        from lazylibrarian.postprocess_utils import enforce_str

        # Test with string (passthrough)
        result = enforce_str("test string")
        self.assertEqual(result, "test string")
        self.assertIsInstance(result, str)

        # Test with ASCII bytes
        result = enforce_str(b"/path/to/file")
        self.assertEqual(result, "/path/to/file")
        self.assertIsInstance(result, str)

        # Test with UTF-8 bytes (special characters)
        result = enforce_str(b"Fran\xc3\xa7ois")  # François in UTF-8
        self.assertIn("Fran", result)
        self.assertIsInstance(result, str)

        # Test with None (should raise)
        with self.assertRaises(ValueError):
            enforce_str(None)

    def test_enforce_bytes(self):
        """Test enforce_bytes wrapper"""
        from lazylibrarian.postprocess_utils import enforce_bytes

        # Test with string
        result = enforce_bytes("test string")
        self.assertEqual(result, b"test string")
        self.assertIsInstance(result, bytes)

        # Test with bytes (passthrough after validation)
        result = enforce_bytes(b"/path/to/file")
        self.assertEqual(result, b"/path/to/file")
        self.assertIsInstance(result, bytes)

        # Test with special characters
        result = enforce_bytes("François")
        self.assertIsInstance(result, bytes)
        # Should contain UTF-8 encoded version
        self.assertIn(b"Fran", result)

        # Test with None (should raise)
        with self.assertRaises(ValueError):
            enforce_bytes(None)

    def test_tokenize_file(self):
        """Test filename tokenization"""
        test_cases = [
            ("/path/to/book.epub", ("book", "epub")),
            ("file.mp3", ("file", "mp3")),
            ("/no/extension", ("extension", "")),
            ("multiple.dots.in.name.pdf", ("multiple.dots.in.name", "pdf")),
        ]

        for filepath, expected in test_cases:
            stem, ext = _tokenize_file(filepath)
            self.assertEqual((stem, ext), expected)

    def test_calculate_fuzzy_match(self):
        """Test fuzzy matching calculation"""
        # Perfect match
        match = _calculate_fuzzy_match("test book", "test book")
        self.assertEqual(match, 100)

        # Partial match (token_set_ratio matches all tokens, so "test" matches "test book" 100%)
        match = _calculate_fuzzy_match("test book", "test")
        self.assertEqual(match, 100)  # token_set_ratio behavior

        # Different tokens
        match = _calculate_fuzzy_match("test book", "other")
        self.assertLess(match, 100)

        # No match
        match = _calculate_fuzzy_match("completely different", "nothing alike")
        self.assertLess(match, 35)

    def test_should_delete_processed_files(self):
        """Test file deletion decision logic"""
        download_dir = "/downloads"

        # .unpack directory - always delete
        should_delete, path = _should_delete_processed_files(
            "/downloads/book.unpack", download_dir
        )
        self.assertTrue(should_delete)
        self.assertIn(".unpack", path)

        # Download root - never delete
        should_delete, path = _should_delete_processed_files("/downloads", download_dir)
        self.assertFalse(should_delete)

    @mock.patch.object(CONFIG, "get_bool")
    def test_should_delete_with_destination_copy(self, mock_get_bool):
        """Test deletion decision with DESTINATION_COPY enabled"""
        mock_get_bool.return_value = True  # DESTINATION_COPY enabled

        should_delete, _ = _should_delete_processed_files(
            "/downloads/subfolder", "/downloads"
        )
        self.assertFalse(should_delete)

    def test_validate_candidate_directory_empty(self):
        """Test validation of empty directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = logging.getLogger(__name__)
            is_valid, reason = _validate_candidate_directory(tmpdir, logger)

            self.assertFalse(is_valid)
            self.assertIn("empty", reason.lower())

    def test_validate_candidate_directory_with_files(self):
        """Test validation of directory with files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test file
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test")

            logger = logging.getLogger(__name__)
            is_valid, reason = _validate_candidate_directory(tmpdir, logger)

            self.assertTrue(is_valid)
            self.assertEqual(reason, "")


class PostprocessMetadataTest(LLTestCaseWithStartup):
    """Test metadata preparation functions"""

    def test_prepare_book_metadata_not_found(self):
        """Test book metadata when book doesn't exist"""
        db = DBConnection()
        try:
            metadata = prepare_book_metadata("nonexistent_id", "eBook", db)
            self.assertIsNone(metadata)
        finally:
            db.close()

    def test_prepare_magazine_metadata_not_found(self):
        """Test magazine metadata when magazine doesn't exist"""
        db = DBConnection()
        try:
            metadata = prepare_magazine_metadata("nonexistent_title", "2025-01-01", db)
            self.assertIsNone(metadata)
        finally:
            db.close()

    def test_prepare_comic_metadata_invalid_id(self):
        """Test comic metadata with invalid ID format"""
        db = DBConnection()
        try:
            # No underscore in ID
            metadata = prepare_comic_metadata("invalidid", db)
            self.assertIsNone(metadata)

            # Empty ID
            metadata = prepare_comic_metadata("", db)
            self.assertIsNone(metadata)
        finally:
            db.close()


class PostprocessFileOperationsTest(LLTestCaseWithStartup):
    """Test file operation helper functions"""

    def test_tokenize_file_with_path_object(self):
        """Test tokenize_file handles Path objects"""
        test_path = Path("/home/user/books/my_book.epub")
        stem, ext = _tokenize_file(str(test_path))

        self.assertEqual(stem, "my_book")
        self.assertEqual(ext, "epub")

    def test_count_zipfiles_in_empty_directory(self):
        """Test counting zipfiles in empty directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            count = _count_zipfiles_in_directory(tmpdir)
            self.assertEqual(count, 0)

    def test_find_valid_file_in_directory_empty(self):
        """Test finding valid file in empty directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = _find_valid_file_in_directory(tmpdir, book_type="ebook")
            self.assertEqual(result, "")

    @mock.patch("lazylibrarian.postprocess_refactor.CONFIG")
    def test_is_valid_media_file(self, mock_config):
        """Test media file validation"""
        mock_config.get_all_types_list.return_value = ["epub", "mobi", "pdf"]
        mock_config.is_valid_booktype.return_value = True

        # Should delegate to CONFIG methods
        result = _is_valid_media_file("test.epub", book_type="ebook")
        self.assertTrue(result)


class PostprocessIntegrationTest(LLTestCaseWithStartup):
    """Integration tests for postprocess workflows"""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.logger = logging.getLogger(__name__)

    def tearDown(self):
        super().tearDown()
        if os.path.exists(self.test_dir):
            import shutil

            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_normalize_and_match_workflow(self):
        """Test complete normalize and fuzzy match workflow"""
        # Simulate a torrent with underscores
        torrent_name = "The_Great_Book_2025"
        expected_name = "The Great Book 2025"

        normalized = _normalize_title(torrent_name)
        self.assertEqual(normalized, "The Great Book 2025")

        # Should match even with different formatting
        match_percent = _calculate_fuzzy_match(
            normalized, _normalize_title(expected_name)
        )
        self.assertEqual(match_percent, 100)

    def test_validate_directory_workflow(self):
        """Test directory validation workflow"""
        # Empty directory should fail
        empty_dir = os.path.join(self.test_dir, "empty")
        os.makedirs(empty_dir)

        is_valid, reason = _validate_candidate_directory(empty_dir, self.logger)
        self.assertFalse(is_valid)
        self.assertIn("empty", reason.lower())

        # Directory with file should pass
        valid_dir = os.path.join(self.test_dir, "valid")
        os.makedirs(valid_dir)
        with open(os.path.join(valid_dir, "book.epub"), "w") as f:
            f.write("test")

        is_valid, reason = _validate_candidate_directory(valid_dir, self.logger)
        self.assertTrue(is_valid)
        self.assertEqual(reason, "")

    def test_bookstate_complete_workflow(self):
        """Test complete BookState workflow"""
        # Create a book state
        book_row = {
            "BookID": "workflow_test",
            "NZBtitle": "Workflow Test Book",
            "NZBmode": "torrent",
            "AuxInfo": "",
            "Completed": str(int(time.time() - 10)),
            "Source": "Deluge",
            "DownloadID": "dl123",
            "NZBurl": "http://test.com/book",
            "NZBprov": "Provider",
            "Status": "Snatched",
            "NZBdate": "2025-12-06 10:00:00",
        }

        book_state = BookState.from_db_row(book_row, CONFIG)

        # Should be completed
        self.assertTrue(book_state.is_completed())

        # Should not need delay (only 10s elapsed vs typical 60s delay)
        should_delay, _ = book_state.should_delay_processing(5)
        self.assertFalse(should_delay)

        # Should be able to delete from client
        self.assertTrue(book_state.can_delete_from_client())

        # Update candidate
        book_state.update_candidate("/downloads/book.epub")
        self.assertTrue(book_state.has_candidate())

        # Test status checks
        self.assertTrue(book_state.is_snatched())
        self.assertTrue(book_state.is_torrent())


class UnprocessedDownloadsTest(LLTestCaseWithStartup):
    """Test unprocessed download handling functions"""

    def test_calculate_download_age_valid(self):
        """Test age calculation with valid date"""
        import datetime

        # 2 hours 15 minutes ago
        past_date = datetime.datetime.now() - datetime.timedelta(hours=2, minutes=15)
        date_str = past_date.strftime("%Y-%m-%d %H:%M:%S")

        hours, mins, secs = _calculate_download_age(date_str)

        self.assertEqual(hours, 2)
        self.assertGreaterEqual(mins, 134)
        self.assertLessEqual(mins, 136)
        self.assertGreaterEqual(secs, 8040)
        self.assertLessEqual(secs, 8160)

    def test_calculate_download_age_invalid(self):
        """Test age calculation with invalid date"""
        hours, mins, secs = _calculate_download_age("invalid date")
        self.assertEqual((hours, mins, secs), (0, 0, 0))

    @mock.patch("lazylibrarian.postprocess_refactor.get_download_folder")
    @mock.patch("lazylibrarian.postprocess_refactor.delete_task")
    @mock.patch("lazylibrarian.postprocess_refactor.CONFIG.get_bool")
    @mock.patch("lazylibrarian.postprocess_refactor.get_list")
    def test_handle_seeding_status_torrent_removed(
        self, mock_get_list, mock_get_bool, mock_delete_task, mock_get_download_folder
    ):
        """Test that torrent removed from client changes status to Snatched"""

        book_state = BookState(
            book_id="book123",
            download_title="Test Book",
            source="TRANSMISSION",
            download_id="dl123",
            progress=-1,  # Not found in client
            finished=False,
            mode_type="torrent",
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        result = _handle_seeding_status(book_state, False, True, mock_db, mock_logger)

        # Should update status to Snatched
        mock_db.action.assert_called_once()
        call_args = mock_db.action.call_args[0]
        self.assertIn("Snatched", call_args[0])
        self.assertEqual(call_args[1], ("book123",))

        # Should skip to next item
        self.assertTrue(result)

    @mock.patch("lazylibrarian.postprocess_refactor.get_download_folder")
    @mock.patch("lazylibrarian.postprocess_refactor.delete_task")
    @mock.patch("lazylibrarian.postprocess_refactor.CONFIG.get_bool")
    @mock.patch("lazylibrarian.postprocess_refactor.get_list")
    @mock.patch("lazylibrarian.postprocess_refactor.now")
    def test_handle_seeding_status_completed(
        self, mock_now, mock_get_list, mock_get_bool, mock_delete_task, mock_get_download_folder
    ):
        """Test that seeding complete changes status to Processed"""

        mock_now.return_value = "2025-12-06 12:00:00"
        mock_get_bool.side_effect = lambda x: False if x == "DEL_COMPLETED" else False
        mock_get_download_folder.return_value = "/downloads/book"
        mock_get_list.return_value = []

        book_state = BookState(
            book_id="book123",
            download_title="Test Book",
            source="TRANSMISSION",
            download_id="dl123",
            progress=100,
            finished=True,
            mode_type="torrent",
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        result = _handle_seeding_status(book_state, False, True, mock_db, mock_logger)

        # Should update status to Processed
        self.assertTrue(
            any("Processed" in str(call) for call in mock_db.action.call_args_list)
        )
        self.assertTrue(result)

    def test_handle_snatched_timeout_slow_magnet(self):
        """Test that downloads < 30 mins with no progress are not aborted"""

        book_state = BookState(
            download_title="Test Book",
            source="QBITTORRENT",
            download_id="dl123",
            progress=-1,
            aux_type="eBook",
            book_id="book123",
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        should_abort, should_skip = _handle_snatched_timeout(
            book_state, 0, 25, 24, mock_db, mock_logger
        )

        self.assertFalse(should_abort)
        self.assertFalse(should_skip)

    def test_handle_snatched_timeout_missing_after_30min(
        self,
    ):
        """Test that downloads > 30 mins with no progress are aborted"""

        book_state = BookState(
            download_title="Test Book",
            source="QBITTORRENT",
            download_id="dl123",
            progress=-1,
            aux_type="eBook",
            book_id="book123",
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        should_abort, should_skip = _handle_snatched_timeout(
            book_state, 1, 35, 24, mock_db, mock_logger
        )

        self.assertTrue(should_abort)
        self.assertFalse(should_skip)

    def test_handle_snatched_timeout_100_percent_no_abort(self):
        """Test that 100% complete timed-out downloads are not aborted"""

        book_state = BookState(
            download_title="Test Book",
            source="QBITTORRENT",
            download_id="dl123",
            progress=100,
            aux_type="eBook",
            book_id="book123",
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        should_abort, should_skip = _handle_snatched_timeout(
            book_state, 25, 1500, 24, mock_db, mock_logger
        )

        # Should not abort 100% downloads, let them retry
        self.assertFalse(should_abort)
        self.assertFalse(should_skip)

    def test_handle_snatched_timeout_stuck_at_90(self):
        """Test that downloads stuck < 95% after timeout are aborted"""

        book_state = BookState(
            download_title="Test Book",
            source="QBITTORRENT",
            download_id="dl123",
            progress=90,
            aux_type="eBook",
            book_id="book123",
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        should_abort, should_skip = _handle_snatched_timeout(
            book_state, 25, 1500, 24, mock_db, mock_logger
        )

        self.assertTrue(should_abort)
        self.assertFalse(should_skip)

    def test_handle_snatched_timeout_95_99_gets_extra_hour(self):
        """Test that downloads at 95-99% get an extra hour"""

        book_state = BookState(
            download_title="Test Book",
            source="QBITTORRENT",
            download_id="dl123",
            progress=97,
            aux_type="eBook",
            book_id="book123",
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        # Just at timeout
        should_abort, _ = _handle_snatched_timeout(
            book_state, 24, 1440, 24, mock_db, mock_logger
        )
        self.assertFalse(should_abort)  # Don't abort yet

        # Past extra hour
        should_abort, _ = _handle_snatched_timeout(
            book_state, 25, 1500, 24, mock_db, mock_logger
        )
        self.assertTrue(should_abort)  # Now abort

    @mock.patch("lazylibrarian.postprocess_refactor.custom_notify_snatch")
    @mock.patch("lazylibrarian.postprocess_refactor.notify_snatch")
    @mock.patch("lazylibrarian.postprocess_refactor.delete_task")
    @mock.patch("lazylibrarian.postprocess_refactor.CONFIG.get_bool")
    def test_handle_aborted_download_sends_notifications(
        self, mock_get_bool, mock_delete_task, mock_notify, mock_custom
    ):
        """Test that aborted downloads send failure notifications"""
        mock_get_bool.return_value = False  # DEL_FAILED = False

        book_state = BookState(
            book_id="book123",
            download_title="Test Book",
            source="TRANSMISSION",
            download_id="dl123",
            status="Aborted",
            aux_type="eBook",
            download_url="http://test.com/book.nzb",
            progress=50,
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        _handle_aborted_download(book_state, 12, mock_db, mock_logger)

        # Should send failure notifications
        mock_custom.assert_called_once_with("book123 TRANSMISSION", fail=True)
        self.assertTrue(mock_notify.called)

    @mock.patch("lazylibrarian.postprocess_refactor.custom_notify_snatch")
    @mock.patch("lazylibrarian.postprocess_refactor.notify_snatch")
    @mock.patch("lazylibrarian.postprocess_refactor.delete_task")
    @mock.patch("lazylibrarian.postprocess_refactor.CONFIG.get_bool")
    def test_handle_aborted_download_updates_status(
        self, mock_get_bool, mock_delete, mock_notify, mock_custom
    ):
        """Test that aborted downloads update status to Failed"""
        mock_get_bool.return_value = False

        book_state = BookState(
            book_id="book123",
            download_title="Test Book",
            source="TRANSMISSION",
            download_id="dl123",
            status="Aborted",
            aux_type="eBook",
            download_url="http://test.com/book.nzb",
            progress=50,
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        _handle_aborted_download(book_state, 12, mock_db, mock_logger)

        # Should update database
        self.assertTrue(mock_db.action.called)
        # Should update wanted to Failed
        calls = [str(call) for call in mock_db.action.call_args_list]
        self.assertTrue(any("Failed" in call for call in calls))

    @mock.patch("lazylibrarian.postprocess_refactor.custom_notify_snatch")
    @mock.patch("lazylibrarian.postprocess_refactor.notify_snatch")
    @mock.patch("lazylibrarian.postprocess_refactor.delete_task")
    @mock.patch("lazylibrarian.postprocess_refactor.CONFIG.get_bool")
    def test_handle_aborted_download_deletes_if_configured(
        self, mock_get_bool, mock_delete_task, mock_notify, mock_custom
    ):
        """Test that when DEL_FAILED=True, aborted downloads are deleted from client"""

        mock_get_bool.return_value = True  # DEL_FAILED = True

        book_state = BookState(
            book_id="book123",
            download_title="Test Book",
            source="QBITTORRENT",
            download_id="dl123",
            status="Aborted",
            aux_type="eBook",
            download_url="http://test.com/book.nzb",
            progress=50,
        )

        mock_db = mock.Mock()
        mock_logger = mock.Mock()

        _handle_aborted_download(book_state, 12, mock_db, mock_logger)

        # Should delete from client
        mock_delete_task.assert_called_once_with("QBITTORRENT", "dl123", True)

    @mock.patch("lazylibrarian.scheduling.schedule_job")
    def test_check_and_schedule_next_run_stop_when_empty(self, mock_schedule):
        """Test that postprocessor stops when no items remain"""
        mock_db = mock.Mock()
        mock_db.select.return_value = []  # No snatched or seeding
        mock_logger = mock.Mock()

        _check_and_schedule_next_run(mock_db, mock_logger, False)

        # Should stop the postprocessor
        from lazylibrarian.scheduling import SchedulerCommand

        mock_schedule.assert_called_once()
        call_args = mock_schedule.call_args
        self.assertEqual(call_args[0][0], SchedulerCommand.STOP)

    @mock.patch("lazylibrarian.scheduling.schedule_job")
    def test_check_and_schedule_next_run_restart_when_seeding(self, mock_schedule):
        """Test that postprocessor restarts when items are seeding"""
        mock_db = mock.Mock()
        mock_db.select.side_effect = [
            [],  # No snatched
            [{"BookID": "123"}],  # Has seeding
        ]
        mock_logger = mock.Mock()

        _check_and_schedule_next_run(mock_db, mock_logger, False)

        # Should restart the postprocessor
        from lazylibrarian.scheduling import SchedulerCommand

        mock_schedule.assert_called_once()
        call_args = mock_schedule.call_args
        self.assertEqual(call_args[0][0], SchedulerCommand.RESTART)
