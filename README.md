## LazyLibrarian
LazyLibrarian is a program to follow authors and grab metadata for all your digital reading needs.
It uses a combination of [OpenLibrary] [Librarything](https://www.librarything.com/) and optionally [GoogleBooks](https://www.googleapis.com/books/v1/) as sources for author info and book info. License: GNU GPL v3

## IMPORTANT NOTE
LazyLibrarian used GoodReads extensively for author and book info, but they have now shut down their api. If you have an existing lazylibrarian installation you will need to move provider. I suggest using OpenLibrary.

You need to create a new clean library and start again
You will lose reading lists
You will lose some wanted books
You will lose goodreads sync
You will lose subscriptions to books/authors/series

# To move provider
1. On LazyLibrarian Manage page, select ebook in dropdown, export csv. select audiobook, export csv. This will keep a list of wanted ebook/audio which we will try to re-import under openlibrary later. Not all will succeed as openlibrary does not have the same range of books as goodreads


2. Lazylibrarian homepage, select all rows per page, select all authors, mark as REMOVE (not delete). This will remove the authors and their books/series from the database but leave the book files etc. Will not affect users/comics/magazines/filters


3. Lazylibrarian config select openlibrary instead of goodreads and save config


4. Lazylibrarian ebook page select Libraryscan and wait for it to complete. Then audiobook page, libraryscan, wait. Audiobook should be much quicker as the authors will mostly already be loaded


5. LazyLibrarian Manage page, ebook, import csv, same for audiobook. This will add back in your "wanted" books


Expect to find many missing authors and books, even ones you own, as openlibrary is not as large a dataset as goodreads yet. I found about 15% of my authors were not on openlibrary, but half a dozen were found that goodreads didn't have


## Description
Right now it's capable of the following:
* Import an existing calibre library (optional)
* Find authors and add them to the database
* List all books of an author and mark ebooks or audiobooks as 'wanted'.
* LazyLibrarian will search for a nzb-file or a torrent or magnet link for that book
* If a nzb/torrent/magnet is found it will be sent to a download client or saved in a black hole where your download client can pick it up.
* Currently supported download clients for usenet are :
- sabnzbd (versions later than 0.7.x preferred)
- nzbget
- synology_downloadstation
* Currently supported download clients for torrent and magnets are:
- deluge
- transmission
- utorrent
- qbittorrent
- rtorrent
- synology_downloadstation
* When processing the downloaded books it will save a cover picture (if available) and save all metadata into metadata.opf next to the bookfile (calibre compatible format)
* The new theme for the site allows it to be accessed from devices with a smaller screen (such as a tablet)
* AutoAdd feature for book management tools like Calibre which must have books in flattened directory structure, or use calibre to import your books into an existing calibre library
* LazyLibrarian can also be used to search for and download magazines, and monitor for new issues

## Install:
LazyLibrarian runs by default on port 5299 at http://localhost:5299

Linux / Mac OS X:

* Install Python 2 v2.6 or higher, or Python 3 v3.5 or higher
* Git clone/extract LL wherever you like
* Run `python LazyLibrarian.py -d` or `python LazyLibrarian.py --daemon` to start in daemon mode
* Fill in all the config (see the docs)


## Documentation:
There is extensive documentation at https://lazylibrarian.gitlab.io/
and a reddit at https://www.reddit.com/r/LazyLibrarian/

Docker tutorial  http://sasquatters.com/lazylibrarian-docker/
Config tutorial  http://sasquatters.com/lazylibrarian-configuration/

## Update
Auto update available via interface from master for git and source installs

## Packages
rpm deb flatpak and snap packages here : https://gitlab.com/LazyLibrarian/LazyLibrarian/tags
These packages do not use the lazylibrarian internal update mechanism.
You can check version from inside lazylibrarian, but to upgrade use the appropriate package manager.
The packages are not updated as regularly as the git/source installations.
NOTE: the smaller flatpak package does not include ghostscript (for magazine cover generation) or calibredb (for calibre communication)
If you need these features, install from source or git, or use the flatpak+ file.
The flatpak+ file includes both ghostscript and calibredb but is considerably larger because of this.
To install: flatpak install lazylibrarian_1.x.x.flatpak. To run: flatpak run org.flatpak.LazyLibrarian
The snap package is confined to users home directory, so all books and downloads need to be accessible from there too.
It should be able to use system installed versions of ghostscript and calibredb provided they are in the system path.
Install the snap package with --devmode eg snap install lazylibrarian_1.7.2_amd64.snap --devmode
AUR package available here: https://aur.archlinux.org/packages/lazylibrarian/
QNAP LazyLibrarian is now available for the QNAP NAS via sherpa. https://forum.qnap.com/viewtopic.php?f=320&t=132373v

## Docker packages
By LinuxServer : https://hub.docker.com/r/linuxserver/lazylibrarian/
By thraxis : https://hub.docker.com/r/thraxis/lazylibrarian-calibre/
The above docker packages both include ghostscript for magazine cover generation and calibredb (via optional variable in LinuxServer version)
LinuxServer version is multi-arch and works on X86_64, armhf and aarch64 (calibredb only available on X86_64)
The dockers can be upgraded using the lazylibrarian internal upgrade mechanism
