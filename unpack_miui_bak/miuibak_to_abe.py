#/bin/env python

import sys

if len(sys.argv)!=2:
    print("usage %s miui_backup_file" % sys.argv[0])
    sys.exit(1) 

f=open(sys.argv[1],'rb')
f.read(0x33)
s=f.read()
ff=open('tmp.jar','wb')
ff.write(s)
ff.close()
f.close()
print("done convert miui_backup_file to abe format")
