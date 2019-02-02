import os
import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.common import notifyStrings, NOTIFY_SNATCH, NOTIFY_DOWNLOAD
from lazylibrarian.formatter import plural
try:
    import apprise
    from apprise import NotifyType, AppriseAsset, Apprise
    lazylibrarian.APPRISE = True
except ImportError as e:
    print(e)
    lazylibrarian.APPRISE = False

class Apprise_Notifier:

    def __init__(self):
        pass

    @staticmethod
    def _sendApprise(self, event=None, message=None, url=None):
        try:
            asset = AppriseAsset()
            asset.default_extension = ".png"
            asset.image_path_mask = "/opt/LazyLibrarian/data/images/{TYPE}-{XY}{EXTENSION}"
            asset.app_id = "LazyLibrarian"
            asset.app_desc = "LazyLibrarian Announcement"
            asset.app_url = "https://gitlab.com/LazyLibrarian/LazyLibrarian"
            apobj = Apprise(asset=asset)
        except Exception as e:
            logger.error(e)
            return False

        if url is not None:
            apobj.add(url)
        else:
            for item in lazylibrarian.APPRISE_PROV:
                if event == notifyStrings[NOTIFY_DOWNLOAD] and item['DOWNLOAD']:
                    apobj.add(item['URL'])
                elif event == notifyStrings[NOTIFY_SNATCH] and item['SNATCH']:
                    apobj.add(item['URL'])
                elif event == 'Test':
                    apobj.add(item['URL'])

        logger.debug(apobj.details()['asset'])
        title = "LazyLibrarian"

        if apobj is None:
            logger.warn("Apprise notifier is not initialised")
            return False

        logger.debug("Apprise: title: " + title)
        logger.debug("Apprise: event: " + event)
        logger.debug("Apprise: message: " + message)
        logger.debug("Apprise: url: " + str(url))
        logger.debug("Using %d notification service%s" % (len(apobj), plural(len(apobj))))

        if event == notifyStrings[NOTIFY_SNATCH]:
            notifytype = NotifyType.INFO
        elif event == notifyStrings[NOTIFY_DOWNLOAD]:
            notifytype = NotifyType.SUCCESS
        else:
            notifytype = NotifyType.WARNING

        return apobj.notify(title=title, body=message, notify_type=notifytype)


    def _notify(self, event, message, url):
        """
        event: The title of the notification to send
        message: The message string to send
        """
        return self._sendApprise(self, event, message, url)

#
# Public functions
#
    def notify_snatch(self, title):
        self._notify(event=notifyStrings[NOTIFY_SNATCH], message=title)

    def notify_download(self, title):
        self._notify(event=notifyStrings[NOTIFY_DOWNLOAD], message=title)

    def test_notify(self, url=None):
        return self._notify(event="Test", message="Testing Apprise settings from LazyLibrarian", url=url)

notifier = Apprise_Notifier
