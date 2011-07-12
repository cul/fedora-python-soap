import getopt
import sys
from urlparse import urlparse
from getpass import getpass
from apim_types import *
from apim_clients import *

### main
opts, args = getopt.getopt(sys.argv[1:],'h:n:s','host=namespace=ssl')
ns = 'demo'
host = None
ssl = False
for o,a in opts:
  if o == '-h':
    host = a
  if o == '-n':
    ns = a
  if o == '-s':
    ssl = True
un = 'fedoraAdmin' # raw_input('username: ')
pw = 'fedoraPassword' # raw_input('password: ')
locator = ServiceLocator()
if host:
  proxy = locator.getAPIM(host=host)
else:
  proxy = locator.getAPIM()
proxy.binding.setAuth(username=un, password=pw)
proxy.binding.setSSL(ssl)
request = getNextPIDRequest()
request.setNumPids(1)
request.setNamespace(ns)
response = proxy.getNextPID(request)
print response.pid[0]
