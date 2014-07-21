def load_cls_from_string(fq_cls_name):
    pivot = fq_cls_name.rfind('.')
    cls_name = fq_cls_name[pivot+1:]
    m = __import__(fq_cls_name[:pivot], fromlist=[cls_name])
    return getattr(m, cls_name)
