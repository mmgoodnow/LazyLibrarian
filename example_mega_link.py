#!/usr/bin/python3
import os
import subprocess
import sys

# login to mega, upload a file, get a link to it, log out again and return link
# progress is logged to stderr, link is returned on stdout
# always exit 0
#
link = ''
if len(sys.argv) != 2:
    print("Usage: link = megalink /path/to/file.pdf")
else:
    f = sys.argv[1]
    params = ["/usr/bin/mega-login", "user@email.com", "megapassword"]
    print(1,params, file=sys.stderr)
    p = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res, err = p.communicate()
    print(2,res, file=sys.stderr)
    params = ["/usr/bin/mega-put", "-c", f]
    print(3,params, file=sys.stderr)
    p = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res, err = p.communicate()
    print(4,res, file=sys.stderr)
    params = ["/usr/bin/mega-export", "-a", "-f", os.path.basename(f)]
    print(5,params, file=sys.stderr)
    p = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res, err = p.communicate()
    print(6,res, file=sys.stderr)
    res = res.decode('utf-8')
    if 'http' in res:
        link = 'http' + res.rsplit('http', 1)[1].strip('\n')
    params = ["/usr/bin/mega-logout"]
    p = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res, err = p.communicate()
    print(7,res, file=sys.stderr)
print(link)
exit(0)
