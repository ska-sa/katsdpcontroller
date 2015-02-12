"""katsdpgraphs library."""

try:
    import pkg_resources as _pkg_resources
    dist = _pkg_resources.get_distribution("katsdpgraphs")
    # ver needs to be a list since tuples in Python <= 2.5 don't have
    # a .index method.
    ver = list(dist.parsed_version)
    __version__ = "r%d" % int(ver[ver.index("*r") + 1])
except (ImportError, _pkg_resources.DistributionNotFound, ValueError, IndexError, TypeError):
    __version__ = "unknown"

