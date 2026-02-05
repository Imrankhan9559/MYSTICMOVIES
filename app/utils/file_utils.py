import math

def format_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

def get_icon_for_mime(mime_type):
    if not mime_type: return "fa-file"
    if "video" in mime_type: return "fa-file-video"
    if "image" in mime_type: return "fa-file-image"
    if "pdf" in mime_type: return "fa-file-pdf"
    if "audio" in mime_type: return "fa-file-audio"
    return "fa-file"