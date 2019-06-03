import os
import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.common import notifyStrings, NOTIFY_SNATCH, NOTIFY_DOWNLOAD
from lazylibrarian.formatter import plural
try:
    import apprise
    from apprise import NotifyType, AppriseAsset, Apprise
    lazylibrarian.APPRISE = getattr(apprise, '__version__', 'Unknown Version')
except ImportError as e:
    lazylibrarian.APPRISE = str(e)


class Apprise_Notifier:

    def __init__(self):
        pass

    @staticmethod
    def _sendApprise(event=None, message=None, url=None):
        try:
            asset = AppriseAsset()
            asset.default_extension = ".png"
            asset.image_path_mask = "/opt/LazyLibrarian/data/images/{TYPE}-{XY}{EXTENSION}"
            asset.image_url_logo = "https://lazylibrarian.gitlab.io/assets/logo.png"
            asset.image_url_mask = "https://lazylibrarian.gitlab.io/assets/{TYPE}-{XY}{EXTENSION}"
            asset.app_id = "LazyLibrarian"
            asset.app_desc = "LazyLibrarian Announcement"
            asset.app_url = "https://gitlab.com/LazyLibrarian/LazyLibrarian"
            apobj = Apprise(asset=asset)
        except Exception as err:
            logger.error(str(err))
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

        if apobj is None:
            logger.warn("Apprise notifier is not initialised")
            return False

        title = "LazyLibrarian"

        logger.debug("Apprise: event: " + event)
        logger.debug("Apprise: message: " + message)
        logger.debug("Apprise: url: " + str(url))
        logger.debug("Using %d notification service%s" % (len(apobj), plural(len(apobj))))
        logger.debug(str(asset.details()))
        if event == notifyStrings[NOTIFY_SNATCH]:
            notifytype = NotifyType.INFO
        elif event == notifyStrings[NOTIFY_DOWNLOAD]:
            notifytype = NotifyType.SUCCESS
        else:
            notifytype = NotifyType.WARNING

        return apobj.notify(title=title, body="%s\n%s" % (event, message), notify_type=notifytype)

    def _notify(self, event, message, url=None):
        """
        event: The title of the notification to send
        message: The message string to send
        url: to send to one notifier. If None send to all enabled notifiers
        """
        return self._sendApprise(event, message, url)

#
# Public functions
#
    def notify_snatch(self, title):
        if lazylibrarian.APPRISE:
            self._notify(event=notifyStrings[NOTIFY_SNATCH], message=title, url=None)
        else:
            return True

    def notify_download(self, title):
        if lazylibrarian.APPRISE:
            self._notify(event=notifyStrings[NOTIFY_DOWNLOAD], message=title, url=None)
        else:
            return True

    def test_notify(self, url=None):
        return self._notify(event="Test", message="Testing Apprise settings from LazyLibrarian", url=url)

    @staticmethod
    def notify_types():
        res = []
        if not lazylibrarian.APPRISE:
            return res
        try:
            apobj = Apprise()
            schemas = apobj.details()['schemas']
            for item in schemas:
                res.append(item['service_name'])
        except (NameError, AttributeError):
            pass
        return res

    @staticmethod
    def version():
        try:
            apobj = Apprise()
            return apobj.details()['version']
        except NameError:
            return ''


notifier = Apprise_Notifier
