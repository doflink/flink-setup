#
# (C) Do Le Quoc, 2015
#

from novaclient.v1_1 import client
import time
import json
import os
import shutil
from os.path import expanduser

from subprocess import call

configfile = 'cloud.auth'

class VirtualMachine:
    def __init__(self):
        pass

    def authenCloud(self, configfile, cloud):
        auth = open(configfile)
        clouds = json.load(auth)
        username = clouds[cloud]["username"]
        password = clouds[cloud]["password"]
        tenant = clouds[cloud]["tenant"]
        url = clouds[cloud]["url"]
        return username, password, tenant, url

    def createKeypair(self, keypair_name, cloud):
        user, password, tenant, url = self.authenCloud(configfile,cloud)
        nova_client = client.Client(user, password, tenant, url)
        if not os.path.exists("./ssh"):
            os.makedirs("./ssh")
        private_key_filename = "./ssh/id_TUD"
        public_key_filename = "./ssh/id_TUD.pub"
        home = expanduser("~")
        keypair = nova_client.keypairs.create(name=keypair_name)

        # Create a file for writing that can only be read and written by owner
        fprivate = os.open(private_key_filename, os.O_WRONLY | os.O_CREAT, 0o600)
        fpublic = os.open(public_key_filename, os.O_WRONLY | os.O_CREAT, 0o600)
        with os.fdopen(fprivate, 'w') as f:
            f.write(keypair.private_key)
        with os.fdopen(fpublic, 'w') as f:
            f.write(keypair.public_key)

        dst = home + "/.ssh/"
        shutil.copy(private_key_filename, dst)
        shutil.copy(public_key_filename, dst)
        private_key = home + "/.ssh/id_TUD"
        call(["ssh-add", private_key])

    def createInstance(self, name, flavor_xs, image, keyname, cloud):
        user, password, tenant, url = self.authenCloud(configfile, cloud)
        nova_client = client.Client(user, password, tenant, url)
        print nova_client
        print flavor_xs
        flavor_xs = nova_client.flavors.find(name=flavor_xs)
        print flavor_xs
        image = nova_client.images.find(name=image)
        instance = nova_client.servers.create(name=name, image=image, flavor=flavor_xs, key_name=keyname)
        return instance

    def listInstances(self, cloud):
        user, password, tenant, url = self.authenCloud(configfile,cloud)
        nova_client = client.Client(user, password, tenant, url)
        print nova_client.servers.list()
        return nova_client.servers.list()
