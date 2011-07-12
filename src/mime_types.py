MIMETYPES = {
  "bmp":"image/bmp",
  "css":"text/css",
  "gif":"image/gif",
  "htm":"text/html",
  "html":"text/html",
  "jpg":"image/jpeg",
  "jpeg":"image/jpeg",
  "jp2":"image/jp2",
  "js":"application/javascript",
  "mov":"video/mov",
  "mpa":"video/mpeg",
  "mpe":"video/mpeg",
  "mpg":"video/mpeg",
  "mpeg":"video/mpeg",
  "mp2":"video/mpeg",
  "pdf":"application/pdf",
  "png":"image/png",
  "qt":"video/quicktime",
  "rtf":"application/rtf",
  "svg":"image/svg+xml",
  "tif":"image/tiff",
  "tiff":"image/tiff",
  "txt":"text/plain",
  "tsv":"text/tab-separated-values",
  "xhtml":"text/xhtml",
  "xml":"text/xml"
}

def mime_from_path(path):
  ext = path.rpartition('.')[2]
  ext = ext.lower()
  if ext in MIMETYPES:
    return MIMETYPES[ext]
  else:
    return "application/octet-stream"
