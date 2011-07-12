from mime_types import *
from apim_types import *
from apim_clients import *
from urlparse import urlparse
import datetime
import base64
import sys
import os, os.path
import codecs
import shutil
import json
import getopt

def testUri(test):
  return str(test).startswith('info:fedora/') or urlparse(test)[0] in ['http','https','file']

class FedoraServices(object):
  def __init__(self,debug=False):
    self.map = dict()
    self.locator = ServiceLocator()
    if debug:
      self.apim = self.locator.getDebug()
      self.apia = self.locator.getAPIA()
    else:
      self.apim = self.locator.getAPIM(host='localhost',port='8443',SSL=True,debug=True)
      self.apia = self.locator.getAPIA(debug=True)
  def setBasicAuth(self, username, password):
    self.apim.binding.setAuth(username=username, password=password)
    self.apia.binding.setAuth(username=username, password=password)
  def setSSL(self, useSSL):
    self.apim.binding.setSSL(useSSL)

  def getRelationships(subject=subject, predicate=predicate):
    request = getRelationshipsRequest(subject=subject, relationship=predicate)
    response = self.apim.getRelationships(request)
    return response

  def addRelationship(subject=subject, predicate=predicate, object=object):
    request = addRelationshipRequest(subject=subject, relationship=predicate, object=object, isLiteral=testUri(object))
    response = self.apim.addRelationship(request)
    return response

  def purgeRelationship(subject=subject, predicate=predicate, object=object):
    request = purgeRelationshipRequest(subject=subject, relationship=predicate, object=object, isLiteral=testUri(object))
    response = self.apim.purgeRelationship(request)
    return response

  def getUriForId(self,identifiers):
    return self.map[identifiers[0].strip()]

  def getChecksumInfo(self, dsLocation):
    if dsLocation.startswith('file://'):
      path = dsLocation.replace('file://','')
      md5Path = path + '.md5'
      if os.access(md5Path, os.R_OK):
        checksum = None
        csType = 'MD5'
        with open(md5Path) as f:
          checksum = f.read().strip()
        return (csType,checksum)
    return (None, None)

  def addDatastream(self, objuri, properties):
    if not("formatURI" in properties):
      properties["formatURI"] = None
    split = objuri.split('/')
    pid = split[-1]
    request = listDatastreamsRequest(pid=pid)
    response = self.apia.listDatastreams(request)
    found = False
    for datastream in response.datastreamDef:
      if datastream.ID == properties["dsID"]:
        found = True
    if not found:
      rclass = addDatastreamRequest
      op = self.apim.addDatastream
    else:
      rclass = modifyDatastreamByReferenceRequest
      op = self.apim.modifyDatastreamByReference
    (csType, cs) = self.getChecksumInfo(properties["dsLocation"])
    if not "MIMEType" in properties:
      properties["MIMEType"] = 'binary/octet-stream'
    request = rclass(pid=pid, dsID=properties["dsID"],
                                     dsLocation=properties["dsLocation"],controlGroup=properties["controlGroup"],
                                     dsState=properties["dsState"],formatURI=properties["formatURI"],
                                     checksumType=csType,checksum=cs,MIMEType=properties["MIMEType"])
    try:
      response = op(request)
      if (response.status == 200):
        print 'SUCCESS: ' + str(op.__class__) +'  ' + objuri + '/' + properties["dsID"] + '=' + properties["dsLocation"]
        return True
      else:
        print 'ERROR: ' + str(op.__class__) +'  ' + objuri + '/' + properties["dsID"] + '=' + properties["dsLocation"]
        return False
    except Exception as ex:
      print 'ERROR: ' + repr(ex)
      print 'ERROR: ' + str(op.__class__) +'  ' + objuri + ' ' + repr(request)
      return False

def usage():
  print 'usage: python ' + sys.argv[0] + ' -i INFILE -u USERNAME -w PASSWORD -p PID [-s USESSL] [-o OUTFILE]'

## MAIN ##

if __name__ == '__main__':
  opts, args = getopt.getopt(sys.argv[1:],'i:o:u:p:w:c:s:')
  infile=False
  user=False
  password=False
  pid=False
  objuri=False
  useSSL = True
  for o,a in opts:
    if o == '-i':
      infile = a
    if o == '-o':
      sys.stdout = open(a, 'w')
    if o == '-u':
      user = a
    if o == '-w':
      password = a
    if o == '-p':
      pid = a
      objuri = 'info:fedora/' + a
    if o == '-s':
      useSSl = (a.lower == 'true')

  if not infile or not os.access(infile, os.R_OK):
    usage()
    exit()
  else:
    fileuri = 'file://' + infile

  myApim = FedoraServices(debug=False)
  if (user):
    myApim.setBasicAuth(username=user,password=password)
  if (useSSL):
    myApim.setSSL(useSSL)
  properties = {'dsID':'CONTENT'}
  properties['dsLocation'] = fileuri
  properties['MIMEType'] = mime_from_path(infile)
  properties['controlGroup'] = 'M'
  properties['dsState'] = 'A'
  properties['formatURI'] = ''
  myApim.addDatastream(pid.strip(),properties)
  if properties['MIMEType'].startswith('image'):
    props = image.triples(infile)
    for triple in props['triples']:
      # search for <objuri> <triple.predicate>, and remove
      oldtriples = myApim.getRelationships(subject=objuri,relationship=triple.predicate)
      for oldtriple in oldtriples:
        myApim.purgeRelationship(subject=objuri,relationship-oldtriple.predicate, object=oldtriple.object, isLiteral = testUri(oldtriple.object))
      # add triples
      myApim.addRelationship(subject=objuri, relationship=triple.predicate, object=triple.object, isLiteral=isUri(triple.object))
  sys.stdout.flush()
