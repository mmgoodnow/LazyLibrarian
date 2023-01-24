#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing parsing XML from providers

import unittest
from xml.etree import ElementTree

from lazylibrarian.config2 import wishlist_type
from lazylibrarian import providers
from unittests.unittesthelpers import LLTestCase


class ProvidersTest(LLTestCase):

    def test_ReturnResultsFieldsBySearchTypeForBook(self):
        book = {"bookid": 'input_bookid', "bookName": 'input_bookname',
                "authorName": 'input_authorname', "searchterm": 'safe_searchterm'}

        newsnabplus_resp = '''<?xml version="1.0" encoding="utf-8"?>
                <rss xmlns:atom="http://www.w3.org/2005/Atom" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/" version="2.0">
                       <channel>
                              <atom:link href="queryhere" rel="self" type="application/rss+xml"></atom:link>
                              <title>usenet-crawler</title>
                              <description>usenet-crawler Feed</description>
                              <link>https://www.usenet-crawler.com/</link>
                              <language>en-gb</language>
                              <webMaster>info@usenet-crawler.com (usenet-crawler)</webMaster>
                              <category></category>
                              <image>
                                     <url>https://www.usenet-crawler.com/templates/default/images/banner.jpg</url>
                                     <title>usenet-crawler</title>
                                     <link>https://www.usenet-crawler.com/</link>
                                     <description>Visit usenet-crawler - A quick usenet indexer</description>
                                 </image>
                              <newznab:response offset="0" total="3292"></newznab:response>
                              <item>
                                     <title>Debbie Macomber - When First They Met (html)</title>
                                     <guid isPermaLink="true">https://www.usenet-crawler.com/details/1c055031d3b32be8e2b9eaee1e33c315</guid>
                                     <link>http</link>
                                     <comments>https://www.usenet-crawler.com/details/1c055031d3b32be8e2b9eaee1e33c315#comments</comments>
                                     <pubDate>Sat, 02 Mar 2013 06:51:28 +0100</pubDate>
                                     <category>Books > Ebook</category>
                                     <description>Debbie Macomber - When First They Met (html)</description>
                                     <enclosure url="http" length="192447" type="application/x-nzb"></enclosure>
                                     <newznab:attr name="category" value="7000"></newznab:attr>
                                     <newznab:attr name="category" value="7020"></newznab:attr>
                                     <newznab:attr name="size" value="192447"></newznab:attr>
                                     <newznab:attr name="guid" value="1c055031d3b32be8e2b9eaee1e33c315"></newznab:attr>
                                 </item>
                          </channel>
                   </rss>                '''
        resultxml = ElementTree.fromstring(newsnabplus_resp)
        nzb = list(resultxml.findall("./channel/item//"))

        result = providers.return_results_by_search_type(book, nzb, search_mode='book', host='hostname')
        self.assertEqual({'bookid': 'input_bookid', 'nzbdate': 'Sat, 02 Mar 2013 06:51:28 +0100', 'nzbtitle':
                          'Debbie Macomber - When First They Met (html)', 'nzbsize': '192447', 'nzburl': 'http',
                          'nzbprov': 'hostname', 'nzbmode': 'book', 'priority': 0}, result)

    def test_ReturnResultsFieldsBySearchTypeForMag(self):
        book = {"bookid": 'input_bookid', "bookName": 'input_bookname',
                "authorName": 'input_authorname', "searchterm": 'safe_searchterm'}

        newsnabplus_resp = '''<?xml version="1.0" encoding="utf-8" ?>
            <rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">
                <channel>
                    <atom:link href="https://www.usenet-crawler.com/api?q=scientific+american&amp;apikey=78c0509bc6bb91742ae0a0b6231e75e4&amp;t=search&amp;extended=1&amp;cat=7020" rel="self" type="application/rss+xml" />
                    <title>usenet-crawler</title>
                    <description>usenet-crawler Feed</description>
                    <link>https://www.usenet-crawler.com/</link>
                    <language>en-gb</language>
                    <webMaster>info@usenet-crawler.com (usenet-crawler)</webMaster>
                    <category></category>
                    <image>
                        <url>https://www.usenet-crawler.com/templates/default/images/banner.jpg</url>
                        <title>usenet-crawler</title>
                        <link>https://www.usenet-crawler.com/</link>
                        <description>Visit usenet-crawler - A quick usenet indexer</description>
                    </image>

                    <newznab:response offset="0" total="3292" />
                    <item>
                        <title>Scientific.American.SCIAM.November.20.3</title>
                        <guid isPermaLink="true">https://www.usenet-crawler.com/details/6814309804e3648c58a9f23345c2a28a</guid>
                        <link>https://www.usenet-crawler.com/getnzb/6814309804e3648c58a9f23345c2a28a.nzb&amp;i=155518&amp;r=78c0509bc6bb91742ae0a0b6231e75e4</link>
                        <comments>https://www.usenet-crawler.com/details/6814309804e3648c58a9f23345c2a28a#comments</comments>
                        <pubDate>Thu, 21 Nov 2013 16:13:52 +0100</pubDate>
                        <category>Books &gt; Ebook</category>
                        <description>Scientific.American.SCIAM.November.20.3</description>
                        <enclosure url="https://www.usenet-crawler.com/getnzb/6814309804e3648c58a9f23345c2a28a.nzb&amp;i=155518&amp;r=78c0509bc6bb91742ae0a0b6231e75e4" length="20811405" type="application/x-nzb" />

                        <newznab:attr name="category" value="7000" />
                        <newznab:attr name="category" value="7020" />
                        <newznab:attr name="size" value="20811405" />
                        <newznab:attr name="guid" value="6814309804e3648c58a9f23345c2a28a" />
                        <newznab:attr name="files" value="4" />
                        <newznab:attr name="poster" value="TROLL &lt;EBOOKS@town.ag&gt;" />

                        <newznab:attr name="grabs" value="10" />
                        <newznab:attr name="comments" value="0" />
                        <newznab:attr name="password" value="0" />
                        <newznab:attr name="usenetdate" value="Thu, 21 Nov 2013 12:13:01 +0100" />
                        <newznab:attr name="group" value="alt.binaries.ebook" />
                    </item>
                </channel>
            </rss>                '''
        # Take the above xml, parse it into element tree, extract the item from it
        # could have just put in item text, but took live example
        resultxml = ElementTree.fromstring(newsnabplus_resp)
        nzb = list(resultxml.findall("./channel/item//"))
        result = providers.return_results_by_search_type(book, nzb, 'hostname', 'mag')
        self.assertEqual(
            {'bookid': 'input_bookid', 'nzbdate': 'Thu, 21 Nov 2013 16:13:52 +0100',
             'nzbtitle': 'Scientific.American.SCIAM.November.20.3', 'nzbsize': '20811405',
             'nzburl': 'https://www.usenet-crawler.com/getnzb/6814309804e3648c58a9f23345c2a28a.nzb&i=155518&r=78c0509bc6bb91742ae0a0b6231e75e4',
             'nzbprov': 'hostname', 'nzbmode': 'mag', 'priority': 0}, result)

    def test_ReturnResultsFieldsBySearchTypeForGeneral(self):
        book = {"bookid": 'input_bookid', "bookName": 'input_bookname',
                "authorName": 'input_authorname', "searchterm": 'safe_searchterm'}

        newsnabplus_resp = '''<?xml version="1.0" encoding="utf-8" ?>
            <rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">
                <channel>
                    <atom:link href="https://www.usenet-crawler.com/api?q=scientific+american&amp;apikey=78c0509bc6bb91742ae0a0b6231e75e4&amp;t=search&amp;extended=1&amp;cat=7020" rel="self" type="application/rss+xml" />
                    <title>usenet-crawler</title>
                    <description>usenet-crawler Feed</description>
                    <link>https://www.usenet-crawler.com/</link>
                    <language>en-gb</language>
                    <webMaster>info@usenet-crawler.com (usenet-crawler)</webMaster>
                    <category></category>
                    <image>
                        <url>https://www.usenet-crawler.com/templates/default/images/banner.jpg</url>
                        <title>usenet-crawler</title>
                        <link>https://www.usenet-crawler.com/</link>
                        <description>Visit usenet-crawler - A quick usenet indexer</description>
                    </image>

                    <newznab:response offset="0" total="3292" />
                    <item>
                        <title>Scientific.American.SCIAM.November.20.3</title>
                        <guid isPermaLink="true">https://www.usenet-crawler.com/details/6814309804e3648c58a9f23345c2a28a</guid>
                        <link>https://www.usenet-crawler.com/getnzb/6814309804e3648c58a9f23345c2a28a.nzb&amp;i=155518&amp;r=78c0509bc6bb91742ae0a0b6231e75e4</link>
                        <comments>https://www.usenet-crawler.com/details/6814309804e3648c58a9f23345c2a28a#comments</comments>
                        <pubDate>Thu, 21 Nov 2013 16:13:52 +0100</pubDate>
                        <category>Books &gt; Ebook</category>
                        <description>Scientific.American.SCIAM.November.20.3</description>
                        <enclosure url="https://www.usenet-crawler.com/getnzb/6814309804e3648c58a9f23345c2a28a.nzb&amp;i=155518&amp;r=78c0509bc6bb91742ae0a0b6231e75e4" length="20811405" type="application/x-nzb" />

                        <newznab:attr name="category" value="7000" />
                        <newznab:attr name="category" value="7020" />
                        <newznab:attr name="size" value="20811405" />
                        <newznab:attr name="guid" value="6814309804e3648c58a9f23345c2a28a" />
                        <newznab:attr name="files" value="4" />
                        <newznab:attr name="poster" value="TROLL &lt;EBOOKS@town.ag&gt;" />

                        <newznab:attr name="grabs" value="10" />
                        <newznab:attr name="comments" value="0" />
                        <newznab:attr name="password" value="0" />
                        <newznab:attr name="usenetdate" value="Thu, 21 Nov 2013 12:13:01 +0100" />
                        <newznab:attr name="group" value="alt.binaries.ebook" />
                    </item>
                </channel>
            </rss>                '''
        # Take the above xml, parse it into element tree, extract the item from it
        # could have just put in item text, but took live example
        resultxml = ElementTree.fromstring(newsnabplus_resp)
        nzb = list(resultxml.findall("./channel/item//"))
        result = providers.return_results_by_search_type(book, nzb, 'hostname', None)
        self.assertEqual(
            {'bookid': 'input_bookid', 'nzbdate': 'Thu, 21 Nov 2013 16:13:52 +0100',
             'nzbtitle': 'Scientific.American.SCIAM.November.20.3', 'nzbsize': '20811405',
             'nzburl': 'https://www.usenet-crawler.com/getnzb/6814309804e3648c58a9f23345c2a28a.nzb&i=155518&r=78c0509bc6bb91742ae0a0b6231e75e4',
             'nzbprov': 'hostname', 'nzbmode': None, 'priority': 0}, result)

    def test_wishlist_type(self):
        provs = [
            ('https://www.goodreads.com/review/list_rss/userid', 'goodreads'),
            ('https://www.goodreads.com/list/show/143500.Best_Books_of_the_Decade_2020_s', 'listopia'),
            ('https://www.goodreads.com/book/show/title', 'listopia'),
            ('https://www.amazon.co.uk/charts', 'amazon'),
            ('https://www.nytimes.com/books/best-sellers/', 'ny_times'),
            ('https://best-books.publishersweekly.com/pw/best-books/2022/top-10', 'publishersweekly'),
            ('https://apps.npr.org/best-books/#year=2022', 'apps.npr.org'),
            ('https://www.penguinrandomhouse.com/books/all-best-sellers', 'penguinrandomhouse'),
            ('https://www.barnesandnoble.com/b/books/_/N-1fZ29Z8q8', 'barnesandnoble'),
            ('https://somewhere-else.com/', '')
        ]
        for p in provs:
            self.assertEqual(wishlist_type(p[0]), p[1])


if __name__ == '__main__':
    unittest.main()
