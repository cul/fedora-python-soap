import xml.parsers.expat
import xml.dom.minidom
import base64
import datetime
NS_SOAP = "http://schemas.xmlsoap.org/soap/envelope/"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_XSD = "http://www.w3.org/2001/XMLSchema"
NS_APIM = "http://www.fedora.info/definitions/1/0/api/"
DEFAULT_CHECKSUM_TYPE = ''

class complexHolder:
  def __init__(self,properties):
    self.properties = properties
  def __getattr__(self, name):
    if name in self.properties:
      return self.properties[name]
    else:
      raise AttributeError(name)

class condition:
  def __init__(self,property,operator,value):
    self.property  = property
    self.operator = operator
    self.value = value
  def toNode(self,document):
    result = document.createElementNS(NS_APIM,'condition')
    property = result.appendChild(document.createElementNS(NS_APIM,'property'))
    operator = result.appendChild(document.createElementNS(NS_APIM,'operator'))
    value = result.appendChild(document.createElementNS(NS_APIM,'value'))
    if self.property != None:
      property.appendChild(document.createTextNode(self.property))
    if self.operator != None:
      operator.appendChild(document.createTextNode(self.operator))
    if self.value != None:
      value.appendChild(document.createTextNode(self.value))
    return result
class apimRequest:
  STUB = U"""\
<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body></soap:Body></soap:Envelope>"""

  def __init__(self, **kw):
    self.document = xml.dom.minidom.parseString(apimRequest.STUB.encode("utf-8"))
  def serialize(self):
    soapBody = self.document.getElementsByTagNameNS(NS_SOAP,'Body')[0]
    query = self.soapBody()
    query.setAttributeNode(self.document.createAttribute('xmlns'))
    query.setAttribute('xmlns', NS_APIM)
    soapBody.appendChild(query)
    return self.document.toxml(encoding="utf-8")
  def stringPart(self,name, value):
    part = self.document.createElementNS(NS_APIM,name)
    if value is None:
      pass
    else:
      _text = self.document.createTextNode(value)
      part.appendChild(_text)
    return part
  def nillify(self, part):
    part.setAttributeNode(self.document.createAttributeNS(NS_XSI,"xsi:nil"))
    part.setAttributeNS(NS_XSI,"xsi:nil",'true')
    return part
  def stringArrayPart(self,name,vals=[]):
    part = self.document.createElementNS(NS_APIM,name)
    for val in vals:
      part.appendChild(self.stringPart("item",val))
    return part
  def typedPart(self,name,value,type):
    part = self.stringPart(name, value)
    part.setAttributeNode(self.document.createAttributeNS(NS_XSI,"type"))
    part.setAttributeNS(NS_XSI,"type",type)
    return part
  def booleanPart(self,name,value):
    value = bool(value)
    if value:
      return self.typedPart(name,'true',"xsd:boolean")
    else:
      return self.typedPart(name,'false',"xsd:boolean")
  def base64Part(self,name,value):
    return self.typedPart(name,value,"xsd:base64Binary")
  def soapBody(self):
    return self.document.createElement(None)

class apimResponse:
  def __init__(self, **kw):
    self.status = 0
  def start_element(self,name,attrs):
    pass
  def end_element(self,name):
    pass
  def char_data(self,data):
    pass
  def parse(self,data):
    parser = xml.parsers.expat.ParserCreate()
    parser.StartElementHandler = self.start_element
    parser.EndElementHandler = self.end_element
    parser.CharacterDataHandler = self.char_data
    parser.Parse(data)

class findObjectsRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#findObjects"
    if "resultFields" in kw:
      self.resultFields = kw["resultFields"]
    else:
      self.resultFields = ["pid"]
    if "maxResults" in kw:
      self.maxResults = int(kw["maxResults"])
    else:
      self.maxResults = 1
    if "query" in kw:
      self.query = kw["query"]
    else:
      self.query = []
  def soapBody(self):
    op = self.document.createElementNS(NS_APIM,"findObjects")
    op.appendChild(self.stringArrayPart("resultFields",self.resultFields))
    op.appendChild(self.typedPart("maxResults",str(self.maxResults),"xsd:nonNegativeInteger"))
    query = self.document.createElementNS(NS_APIM,"query")
    conditions = self.document.createElementNS(NS_APIM,"conditions")
    terms = self.document.createElementNS(NS_APIM,"terms")
    qt = 0
    for q in self.query:
      if q.__class__ == condition:
        conditions.appendChild(q.toNode(self.document))
        qt += 1
      else:
        terms.appendChild(self.document.createTextNode(str(q)))
        qt += 2
    if (qt == 0):
      print "Warning- no query to findObjects"
    if (qt == 1):
      query.appendChild(conditions)
    if (qt == 2):
      query.appendChild(terms)
    if (qt == 3):
      print "Error- terms and conditions in query to findObjects"
    op.appendChild(query)
    return op
        
class findObjectsResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self, **kw)
    self.resultList = []
    self.buffering = False
    self.buffer = None
    self.propsBuffer = None
  def start_element(self,name,attrs):
    if name == 'objectFields':
      self.buffering = True
      self.propsBuffer = {}
    else:
      if self.buffering:
        self.buffer = ''
  def end_element(self,name):
    if name == 'objectFields':
      self.resultList.append(complexHolder(self.propsBuffer))
      self.buffering = False
      self.propsBuffer = None
    else:
      if self.buffering:
        if name not in self.propsBuffer:
          self.propsBuffer[name] = []
        self.propsBuffer[name].append(self.buffer)
        self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class getObjectProfileRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#getObjectProfile"
    if ("pid" in kw):
      self.pid = kw["pid"]
    else:
      self.pid = None
    if ("asOfDateTime" in kw):
      self.asOfDateTime = kw["asOfDateTime"]
    else:
      self.asOfDateTime = None
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"getObjectProfile")
     op.appendChild(self.stringPart("pid",self.pid))
     op.appendChild(self.stringPart("asOfDateTime",self.asOfDateTime))
     return op
class getObjectProfileResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self, **kw)
    self.props = {}
    self.objModels = []
    self.buffering = False
  def start_element(self,name,attrs):
    if name == 'objModels':
      self.buffering = True
    else:
      self.buffer = ''
  def end_element(self,name):
    if self.buffering:
      if name == 'objModels':
        self.buffering = False
      else:
        self.objModels.append(self.buffer)
        self.buffer = None
    else:
      self.props[name] = self.buffer
      self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data
  def __getattr__(self,name):
    return self.props[name]

class listDatastreamsRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#listDatastreams"
    if ("pid" in kw):
      self.pid = kw["pid"]
    else:
      self.pid = None
    if ("asOfDateTime" in kw):
      self.asOfDateTime = kw["asOfDateTime"]
    else:
      self.asOfDateTime = None
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"listDatastreams")
     op.appendChild(self.stringPart("pid",self.pid))
     op.appendChild(self.stringPart("asOfDateTime",self.asOfDateTime))
     return op

class listDatastreamsResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self, **kw)
    self.datastreamDef = []
    self.buffering = False
  def start_element(self,name,attrs):
    if name == 'datastreamDef':
      self.buffering = True
      self.propsBuffer = {}
    else:
      if self.buffering:
        self.buffer = ''
  def end_element(self,name):
    if name == 'datastreamDef':
      self.datastreamDef.append(complexHolder(self.propsBuffer))
      self.buffering = False
      self.propsBuffer = None
    else:
      if self.buffering:
        self.propsBuffer[name] = self.buffer
        self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data
    
class ingestRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#ingest"
    self.format = "info:fedora/fedora-system:FOXML-1.1"
    if ("objectXml" in kw):
      self.objectXml = kw["objectXml"]
    else:
      self.objectXml = None
    if ("logMessage" in kw):
      self.logMessage = kw["LogMessage"]
    else:
      self.logMessage = "Batch Load"
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"ingest")
     op.appendChild(self.stringPart("objectXml",base64.b64encode(self.objectXml.encode('utf-8'))))
     op.appendChild(self.stringPart("format",self.format))
     op.appendChild(self.stringPart("logMessage",self.logMessage))
     return op

class ingestResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
  def start_element(self,name,attrs):
    if name == 'objectPID':
      self.buffer = ''
  def end_element(self,name):
    if name == 'objectPID':
      self.objectPID = self.buffer
      self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class modifyObjectRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#modifyObject"
    if ("pid" in kw):
      self.pid = kw["pid"]
    else:
      self.pid = None
    if ("state" in kw):
      self.state = kw["state"]
    else:
      self.state = None
    if ("label" in kw):
      self.label = kw["label"]
    else:
      self.label = None
    if ("ownerId" in kw):
      self.ownerId = kw["ownerId"]
    else:
      self.ownerId = 'fedoraAdmin'
    if ("logMessage" in kw):
      self.logMessage = kw["logMessage"]
    else:
      now = datetime.datetime.now()
      self.logMessage = 'modified ' + now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"modifyObject")
     op.appendChild(self.stringPart("pid",self.pid))
     state = op.appendChild(self.stringPart("state",self.state))
     if (self.state == None):
       self.nillify(state)
     label = op.appendChild(self.stringPart("label",self.label))
     if (self.label == None):
       self.nillify(label)
     op.appendChild(self.stringPart("ownerId",self.ownerId))
     op.appendChild(self.stringPart("logMessage",self.logMessage))
     return op

class modifyObjectResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
  def start_element(self,name,attrs):
    if name == 'modifiedDate':
      self.buffer = ''
  def end_element(self,name):
    if name == 'modifiedDate':
      self.modifiedDate = self.buffer
      self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class getObjectXMLRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)

class getObjectXMLResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)

class exportRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)

class exportResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)

class purgeObjectRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)

class purgeObjectResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)

class addDatastreamRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#addDatastream"
    if ("pid" in kw):
      self.pid = kw["pid"]
    else:
      self.pid = None
    if ("dsID" in kw):
      self.dsID = kw["dsID"]
    else:
      self.dsID = None
    self.altIDs = []
    if ("dsLabel" in kw):
      self.dsLabel = kw["dsLabel"]
    else:
      self.dsLabel = None
    if ("versionable" in kw):
      self.versionable = bool(kw["versionable"])
    else:
      self.versionable = True
    if ("MIMEType" in kw):
      self.MIMEType = kw["MIMEType"]
    else:
      self.MIMEType = "binary/octet-stream"
    if ("formatURI" in kw):
      self.formatURI = kw["formatURI"]
    else:
      self.formatURI = None
    if ("dsLocation" in kw):
      self.dsLocation = kw["dsLocation"]
    else:
      self.dsLocation = None
    if ("controlGroup" in kw):
      self.controlGroup = kw["controlGroup"]
    else:
      self.controlGroup = 'M'
    if ("dsState" in kw):
      self.dsState = kw["dsState"]
    else:
      self.dsState = 'I'
    if ("checksum" in kw):
      self.checksum = kw['checksum']
    else:
      self.checksum = None
    if ("checksumType" in kw):
      self.checksumType = kw["checksumType"]
    else:
      self.checksumType = None
    if ("logMessage" in kw):
      self.logMessage = kw["logMessage"]
    else:
      self.logMessage = 'Adding ' + str(self.dsID) + ' datastream to ' + str(self.pid)
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"addDatastream")

     op.appendChild(self.stringPart("pid",self.pid))
     op.appendChild(self.stringPart("dsID",self.dsID))
     op.appendChild(self.stringArrayPart("altIDs",self.altIDs))
     op.appendChild(self.stringPart("dsLabel",self.dsLabel))
     op.appendChild(self.booleanPart("versionable",self.MIMEType))
     op.appendChild(self.stringPart("MIMEType",self.MIMEType))
     op.appendChild(self.stringPart("formatURI",self.formatURI))
     op.appendChild(self.stringPart("dsLocation",self.dsLocation))
     op.appendChild(self.stringPart("controlGroup",self.controlGroup))
     op.appendChild(self.stringPart("dsState",self.dsState))
     cstpart = op.appendChild(self.stringPart("checksumType",self.checksumType))
     cspart = op.appendChild(self.stringPart("checksum",self.checksum))
     if (self.checksumType is None):
       self.nillify(cstpart)
     if (self.checksum is None):
       self.nillify(cspart)
     op.appendChild(self.stringPart("logMessage",self.logMessage))
     return op

class addDatastreamResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
  def start_element(self,name,attrs):
    if name == 'datastreamID':
      self.buffer = ''
  def end_element(self,name):
    if name == 'datastreamID':
      self.datastreamID = self.buffer
      self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class modifyDatastreamByReferenceRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#modifyDatastreamByReference"
    if ("pid" in kw):
      self.pid = kw["pid"]
    else:
      self.pid = None
    if ("dsID" in kw):
      self.dsID = kw["dsID"]
    else:
      self.dsID = None
    self.altIDs = []
    if ("dsLabel" in kw):
      self.dsLabel = kw["dsLabel"]
    else:
      self.dsLabel = None
    if ("force" in kw):
      self.force = bool(kw["force"])
    else:
      self.force = True
    if ("MIMEType" in kw):
      self.MIMEType = kw["MIMEType"]
    else:
      self.MIMEType = "binary/octet-stream"
    if ("formatURI" in kw):
      self.formatURI = kw["formatURI"]
    else:
      self.formatURI = None
    if ("dsLocation" in kw):
      self.dsLocation = kw["dsLocation"]
    else:
      self.dsLocation = None
    if ("checksumType" in kw):
      self.checksumType = kw["checksumType"]
    else:
      self.checksumType = None
    if ("checksum" in kw):
      self.checksum = kw["checksum"]
    else:
      self.checksum = None
    if ("logMessage" in kw):
      self.logMessage = kw["logMessage"]
    else:
      self.logMessage = 'Adding ' + str(self.dsID) + ' datastream to ' + str(self.pid)
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"modifyDatastreamByReference")

     op.appendChild(self.stringPart("pid",self.pid))
     op.appendChild(self.stringPart("dsID",self.dsID))
     op.appendChild(self.stringArrayPart("altIDs",self.altIDs))
     op.appendChild(self.stringPart("dsLabel",self.dsLabel))
     op.appendChild(self.stringPart("MIMEType",self.MIMEType))
     op.appendChild(self.stringPart("formatURI",self.formatURI))
     op.appendChild(self.stringPart("dsLocation",self.dsLocation))
     cstpart = op.appendChild(self.stringPart("checksumType",self.checksumType))
     cspart = op.appendChild(self.stringPart("checksum",self.checksum))
     if (self.checksumType is None):
       self.nillify(cstpart)
     if (self.checksum is None):
       self.nillify(cspart)
     op.appendChild(self.stringPart("logMessage",self.logMessage))
     op.appendChild(self.booleanPart("force",self.force))
     return op

class modifyDatastreamByReferenceResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
  def start_element(self,name,attrs):
    if name == 'modifiedDate':
      self.buffer = ''
  def end_element(self,name):
    if name == 'modifiedDate':
      self.modifiedDate = self.buffer
      self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class modifyDatastreamByValueRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#modifyDatastreamByValue"
    if ("pid" in kw):
      self.pid = kw["pid"]
    else:
      self.pid = None
    if ("dsID" in kw):
      self.dsID = kw["dsID"]
    else:
      self.dsID = None
    self.altIDs = []
    if ("dsLabel" in kw):
      self.dsLabel = kw["dsLabel"]
    else:
      self.dsLabel = None
    if ("force" in kw):
      self.force = bool(kw["force"])
    else:
      self.force = True
    if ("MIMEType" in kw):
      self.MIMEType = kw["MIMEType"]
    else:
      self.MIMEType = "binary/octet-stream"
    if ("formatURI" in kw):
      self.formatURI = kw["formatURI"]
    else:
      self.formatURI = None
    if ("dsContent" in kw):
      self.dsContent = base64.b64encode(kw["dsContent"].encode("utf-8"))
    else:
      self.dsContent = None
    if ("checksumType" in kw):
      self.checksumType = kw["checksumType"]
    else:
      self.checksumType = None
    if ("checksum" in kw):
      self.checksum = kw["checksum"]
    else:
      self.checksum = None
    if ("logMessage" in kw):
      self.logMessage = kw["logMessage"]
    else:
      self.logMessage = 'Adding ' + str(self.dsID)+ ' datastream to ' + str(self.pid)
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"modifyDatastreamByValue")

     op.appendChild(self.stringPart("pid",self.pid))
     op.appendChild(self.stringPart("dsID",self.dsID))
     op.appendChild(self.stringArrayPart("altIDs",self.altIDs))
     op.appendChild(self.stringPart("dsLabel",self.dsLabel))
     op.appendChild(self.stringPart("MIMEType",self.MIMEType))
     op.appendChild(self.stringPart("formatURI",self.formatURI))
     op.appendChild(self.base64Part("dsContent",self.dsContent))
     cstpart = op.appendChild(self.stringPart("checksumType",self.checksumType))
     cspart = op.appendChild(self.stringPart("checksum",self.checksum))
     if (self.checksumType is None):
       self.nillify(cstpart)
     if (self.checksum is None):
       self.nillify(cspart)
     op.appendChild(self.stringPart("logMessage",self.logMessage))
     op.appendChild(self.booleanPart("force",self.force))
     return op

class modifyDatastreamByValueResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
  def start_element(self,name,attrs):
    if name == 'modifiedDate':
      self.buffer = ''
  def end_element(self,name):
    if name == 'modifiedDate':
      self.modifiedDate = self.buffer
      self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class setDatastreamStateRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)

class setDatastreamStateResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)

class setDatastreamVersionableRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)

class setDatastreamVersionableResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)

class compareDatastreamChecksumRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)

class compareDatastreamChecksumResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)

class getDatastreamRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#getDatastream"
    if "pid" in kw:
      self.pid = kw["pid"]
    else:
      self.pid = None
    if "asOfDateTime" in kw:
      self.asOfDateTime = kw["asOfDateTime"]
    else:
      self.asOfDateTime = None
    if "dsID" in kw:
      self.dsID = kw["dsID"]
    else:
      self.dsID = None
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"getDatastream")
     op.appendChild(self.stringPart("pid",self.pid)) 
     op.appendChild(self.stringPart("asOfDateTime",self.asOfDateTime)) 
     op.appendChild(self.stringPart("dsID",self.dsID))
     return op

class getDatastreamResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
    self.datastream = None
  def start_element(self,name,attrs):
    if name == 'datastream':
      self.buffering = True
      self.propsBuffer = {}
    else:
      if self.buffering:
        self.buffer = ''
  def end_element(self,name):
    if name == 'datastream':
      self.datastream = complexHolder(self.propsBuffer)
      self.buffering = False
      self.propsBuffer = None
    else:
      if self.buffering:
        self.propsBuffer[name] = self.buffer
        self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class getDatastreamsRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#getDatastreams"
    if "pid" in kw:
      self.pid = kw["pid"]
    else:
      self.pid = None
    if "asOfDateTime" in kw:
      self.asOfDateTime = kw["asOfDateTime"]
    else:
      self.asOfDateTime = None
    if "dsState" in kw:
      self.dsState = kw["dsState"]
    else:
      self.dsState = 'A'
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"getDatastreams")
     op.appendChild(self.stringPart("pid",self.pid)) 
     op.appendChild(self.stringPart("asOfDateTime",self.asOfDateTime)) 
     op.appendChild(self.stringPart("dsState",self.dsState))
     return op

class getDatastreamsResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
    self.datastream = []
    self.buffering = False
    self.propsBuffer = None
  def start_element(self,name,attrs):
    if name == 'datastream':
      self.buffering = True
      self.propsBuffer = {}
    else:
      if self.buffering:
        self.buffer = ''
  def end_element(self,name):
    if name == 'datastream':
      self.datastream.append(complexHolder(self.propsBuffer))
      self.buffering = False
      self.propsBuffer = None
    else:
      if self.buffering:
        self.propsBuffer[name] = self.buffer
        self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class getDatastreamHistoryRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#getDatastreamHistory"
    if "pid" in kw:
      self.pid = kw["pid"]
    else:
      self.pid = None
    if "dsID" in kw:
      self.dsID = kw["dsID"]
    else:
      self.dsID = None
  def soapBody(self):
     op = self.document.createElementNS(NS_APIM,"getDatastreamHistory")
     op.appendChild(self.stringPart("pid",self.pid)) 
     op.appendChild(self.stringPart("dsID",self.dsID))
     return op

class getDatastreamHistoryResponse(getDatastreamsResponse):
  def __init__(self, **kw):
    getDatastreamsResponse.__init__(self,**kw)

class purgeDatastreamRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)

class purgeDatastreamResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)

class getNextPIDRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self, **kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#getNextPID"
    if ("numPIDs" in kw):
      self.numPIDs = kw["numPIDs"]
    else:
      self.numPids = 1
    if ("pidNamespace" in kw):
      self.pidNamespace = kw["pidNamespace"]
    else:
      self.pidNamespace = 'changeme'
  def setNumPids(self,num):
    self.numPids = num
  def setNamespace(self,ns):
    self.ns = ns
  def soapBody(self):
    op = self.document.createElementNS(NS_APIM,"getNextPID")
    op.appendChild(self.stringPart("numPIDs",str(self.numPIDs)))
    op.appendChild(self.stringPart("pidNamespace",self.pidNamespace))
    return op

class getNextPIDResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
    self.pid = []
  def start_element(self,name,attrs):
    if name == 'pid':
      self.buffer = ''
  def end_element(self,name):
    if name == 'pid':
      self.pid.append(self.buffer)
      self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class getRelationshipsRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#getRelationships"
    if ("subject" in kw):
      self.pid = kw["subject"]
    else:
      self.pid = None
    if ("relationship" in kw):
      self.relationship = kw["relationship"]
    else:
      self.relationship = None
  def soapBody(self):
    op = self.document.createElementNS(NS_APIM,"getRelationships")
    op.appendChild(self.stringPart("subject",str(self.subject)))
    op.appendChild(self.stringPart("relationship",self.relationship))
    return op

class getRelationshipsResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
    self.relationships = []
    self.buffering = False
    self.buffer = None
    self.propsBuffer = None
  def start_element(self,name,attrs):
    if name == 'relationships':
      self.buffering = True
      self.propsBuffer = {}
    else:
      if self.buffering:
        self.buffer = ''
  def end_element(self,name):
    if name == 'relationships':
      self.resultList.append(complexHolder(self.propsBuffer))
      self.buffering = False
      self.propsBuffer = None
    else:
      if self.buffering:
        if name not in self.propsBuffer:
          self.propsBuffer[name] = []
        self.propsBuffer[name].append(self.buffer)
        self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data


class addRelationshipRequest(apimRequest):
  def __init__(self, **kw):
    apimRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#addRelationship"
    if ("isLiteral" in kw):
      self.isLiteral = bool(kw["isLiteral"])
    else:
      self.literal = False
    if ("pid" in kw):
      self.pid = kw["pid"]
    else:
      self.pid = None
    if ("relationship" in kw):
      self.relationship = kw["relationship"]
    else:
      self.relationship = None
    if ("object" in kw):
      self.object = kw["object"]
    else:
      self.object = None
    if ("datatype" in kw):
      self.datatype = kw["datatype"]
    else:
      self.datatype = None
  def soapBody(self):
    op = self.document.createElementNS(NS_APIM,"addRelationship")
    if (self.isLiteral): # do something with datatype?
      pass
    op.appendChild(self.stringPart("pid",self.pid))
    op.appendChild(self.stringPart("relationship",self.relationship))
    op.appendChild(self.stringPart("object",self.object))
    op.appendChild(self.booleanPart("isLiteral",self.isLiteral))
    op.appendChild(self.stringPart("datatype",self.datatype))
    return op

class addRelationshipResponse(apimResponse):
  def __init__(self, **kw):
    apimResponse.__init__(self,**kw)
    self.added = False
  def start_element(self,name,attrs):
    if name == 'added':
      self.buffer = ''
  def end_element(self,name):
    if name == 'added':
      self.added = (self.buffer == 'true')
      self.buffer = None
  def char_data(self,data):
    if (self.buffer != None):
      self.buffer += data

class purgeRelationshipRequest(addRelationshipRequest):
  def __init__(self, **kw):
    addRelationshipRequest.__init__(self,**kw)
    self.soapaction="http://www.fedora.info/definitions/1/0/api/#purgeRelationship"
  def soapBody(self):
    op = self.document.createElementNS(NS_APIM,"purgRelationship")
    if (self.isLiteral): # do something with datatype?
      pass
    op.appendChild(self.stringPart("pid",self.pid))
    op.appendChild(self.stringPart("relationship",self.relationship))
    op.appendChild(self.stringPart("object",self.object))
    op.appendChild(self.booleanPart("isLiteral",self.isLiteral))
    op.appendChild(self.stringPart("datatype",self.datatype))
    return op
class purgeRelationshipResponse(addRelationshipResponse):
  def __init__(self, **kw):
    addRelationshipResponse.__init__(self,**kw)
