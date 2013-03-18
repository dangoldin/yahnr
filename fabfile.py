from __future__ import with_statement
from fabric.api import *
from contextlib import contextmanager as _contextmanager
import datetime, os
import config

all_hosts = [ config.SERVER_URL, ]

env.directory = config.SERVER_PATH
env.activate = 'source %svenv/bin/activate' % config.SERVER_PATH

env.roledefs = {
    'web': [ config.SERVER_URL, ],
}

@_contextmanager
def virtualenv():
    with cd(env.directory):
        with prefix(env.activate):
            yield

@hosts(env.roledefs['web'])
def deploy_web():
    with virtualenv():
        run("git pull")

def deploy():
    execute('deploy_web', roles=['web'])