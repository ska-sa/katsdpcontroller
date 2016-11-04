from __future__ import print_function, division, absolute_import
import networkx
import .scheduler


def make_graph():
    g = scheduler.LogicalGraph()
    telstate = scheduler.Node('sdp.telstate')
    telstate.cpus = 0.1
    telstate.mem = 8192     # It's an in-memory database, so give it a decent amount
    telstate.image = 'redis'
    telstate.ports = 1
    telstate.docker_options = ['--publish={ports[0]}:6379']
    telstate.config = {'db_key': 0}
    g.add_node(telstate)
