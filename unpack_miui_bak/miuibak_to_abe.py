# /bin/env python

import sys
import os

if len(sys.argv) != 2:
    print("usage %s miui_backup_file" % sys.argv[0])
    sys.exit(1)

f = open(sys.argv[1], 'rb')
s = f.read(100)
pos = s.find(b'ANDROID BACKUP')
if pos == -1:
    print("signature not found")
    sys.exit(2)
f.seek(pos)
s = f.read()
ff = open('tmp.ab', 'wb')
ff.write(s)
ff.close()
f.close()
print("done convert miui_backup_file to abe format")

os.system("java -jar abe.jar unpack tmp.ab tmp.tar")
