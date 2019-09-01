#!/usr/bin/python
# NOTE make sure the above path to python is correct for your environment
import os
import subprocess

converter = "ebook-convert"  # if not in your "path", put the full pathname here
books_parent_dir = '/home/phil/Test_Library'  # change to your dir

for root, subFolders, files in os.walk(books_parent_dir):
    for name in files:
        for source, dest in [['.mobi', '.epub'], ['.epub', '.mobi']]:
            if name.endswith(source):
                source_file = (os.path.join(root, name))
                dest_file = source_file[:-len(source)] + dest
                if not os.path.exists(dest_file):
                    params = [converter, source_file, dest_file]
                    try:
                        print("Creating %s for %s" % (dest, name))
                        res = subprocess.check_output(params, stderr=subprocess.STDOUT)
                    except Exception as e:
                        print("%s\n" % e)

