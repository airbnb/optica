from fabric.api import env, run, sudo, put

import collections
import json
import requests

response  = requests.get('http://%s:8080/roles' % optica_ip)
hosts     = json.loads(response.text)

# fill the role list
env.roledefs = collections.defaultdict(lambda: [])
for hostinfo in hosts['nodes'].values():
  env.roledefs[hostinfo['role']].append(hostinfo['hostname'])

# show the roll list if no role selected
if not env.roles:
  print "Available roles:\n"
  for role in sorted(env.roledefs.keys()):
    count = len(env.roledefs[role])
    print "    %-30s %3d machine%s" % (role, count, "s" if count > 1 else "")
  print ""

def uptime():
  """Check the uptime on a node"""
  run('uptime')

def restart_service(service_name):
  """Restart a specified service (e.g. `fab restart_service:nginx`)"""
  sudo('service %s restart' % service_name)
