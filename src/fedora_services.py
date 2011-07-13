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
                     dsLocation=properties["dsLocation"],
                     controlGroup=properties["controlGroup"],
                     dsState=properties["dsState"],
                     formatURI=properties["formatURI"],
                     checksumType=csType,checksum=cs,
                     MIMEType=properties["MIMEType"])
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

## MAIN ##

if __name__ == '__main__':
  pass
