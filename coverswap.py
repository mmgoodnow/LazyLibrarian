#!/usr/bin/python
# NOTE make sure the above path to python is correct for your environment
# There should be one parameter, the name of a magazine file to swap pages.
#
import sys
import os

try:
    from PyPDF3 import PdfFileWriter, PdfFileReader
except ImportError:
    sys.stderr.write("PyPDF3 not found\n")
    exit(1)

if len(sys.argv) != 2:
    sys.stderr.write('Usage: coverswap "filename.pdf\n"')
    exit(1)

sourcefile = sys.argv[1]
_, extn = os.path.splitext(sourcefile)
if extn.lower() != '.pdf':
    sys.stderr.write('Usage: coverswap "filename.pdf"\n')
    exit(1)

try:
    output = PdfFileWriter()
    input1 = PdfFileReader(open(sourcefile, "rb"))
    cnt = input1.getNumPages()
    output.addPage(input1.getPage(1))
    output.addPage(input1.getPage(0))
    p = 2
    while p < cnt:
        output.addPage(input1.getPage(p))
        p = p + 1
    with open(sourcefile + 'new', "wb") as outputStream:
        output.write(outputStream)
    os.remove(sourcefile)
    os.rename(sourcefile + 'new', sourcefile)
    sys.stdout.write("%s has %d pages. Swapped pages 1 and 2\n" % (sourcefile, cnt))
    exit(0)
except Exception as e:
    sys.stderr.write("%s\n" % e)
    exit(1)

