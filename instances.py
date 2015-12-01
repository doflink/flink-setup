#
# (C) Do Le Quoc, 2015
#

from openstack import VM
import json

import os

import argparse
from os.path import expanduser
from time import gmtime, strftime
import logging
import time

def createMachine(name, flavor, image, keyname):
    vm = VirtualMachine()
    print name, flavor, image
    vm.createInstance(name, flavor, image, keyname)


def getVMsIPandName(vm, cloud):
    vms={}
    servers = vm.listInstances(cloud)
    for server in servers:
        for network in server.networks['private']:
             vms[server.name] = network

    return vms


def changesshConfig(vm, cloud):
    home = expanduser("~")
    sshconfigpath = home + "/.ssh/config"
    f=open(sshconfigpath, "a+")
    fvms = open("vms/vms.json")
    vms = json.load(fvms)
    username, password, tenant, url = vm.authenCloud("cloud.auth", cloud)
    for vm  in vms:
        #print vm, vms[vm]
        f.write("Host " + vm + "\n")
        link = url.split(":")[1]
        domain = link.split("-")[1]
        #print domain
        f.write("ProxyCommand ssh forward@ssh." + tenant + "." + domain + " nc -q0 " + vms[vm] + " 22" + "\n")
        f.write("ForwardAgent yes \n\n")
    f.close()


def vms2json(vm, cloud):
    vms = getVMsIPandName(vm, cloud)
    if not os.path.exists("./vms"):
        os.makedirs("./vms")
    with open('vms/vms.json', 'w') as outfile:
        json.dump(vms, outfile)


def createLogger():
    if not os.path.exists("./logs"):
        os.makedirs("./logs")
    timelog = strftime("%Y-%m-%d-%H-%M-%S", gmtime())
    loggername = "./logs/" + timelog +  '-TUDcrawler'
    logger = logging.getLogger(loggername)
    hdlr = logging.FileHandler(loggername + '.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger


if __name__ == "__main__":

    #instances = VirtualMachine()
    #vm.createKeypair("tud")
    #vm.createInstance("crawler1", "cloudcompute.xxl", "Ubuntu 12.04 LTS x64", "crawler")
    #vm.createInstance("crawler1", "cloudcompute.l", "Ubuntu 12.04 LTS x64", "tud")
    #vm.createInstance("crawler2", "cloudcompute.l", "Ubuntu 12.04 LTS x64", "tud")
    #vm.createInstance("crawler3", "cloudcompute.l", "Ubuntu 12.04 LTS x64", "tud")

    parser = argparse.ArgumentParser(description='Deploying a Flink Hadoop Spark system on a OpenStack based cloud')
    parser.add_argument('-c','--cloud', type=str, default="Cloud0",
                       help='Identifying the cloud name')
    parser.add_argument('-n', '--vms',  type=int, default=2,
                       help='Number of Vms for the deploying')
    parser.add_argument('-k','--keypair', type=str, default='TUD',
                       help='Name of a keypair for VMs authentication ')
    parser.add_argument('-f','--flavor', type=str, default='cloudcompute.m',
                       help='Type of VM')
    parser.add_argument('-i','--image', type=str, default='Ubuntu 12.04 LTS x64',
                       help='Operating System type')

    args = parser.parse_args()
    #print args.vms
    logger = createLogger()
    flavor = args.flavor
    keypair = args.keypair
    image = args.image
    cloud = args.cloud
    deploy = "Deploying on cloud:  " + cloud  + ", Number of VMs: " + str(args.vms) + ", flavor: " + flavor + ", image: " + image + ", keypair: " + keypair + "\n" 
    logger.info(deploy)
    vm = VirtualMachine()
    
    try:
        print("##### Creating VMs on the OpenStack system: \n")
        vm.createKeypair(keypair, cloud)
        for id in range(args.vms):
            hostname = "crawler" + str(id)
            vm.createInstance(hostname, flavor, image, keypair, cloud)
        time.sleep(200)
        vms2json(vm, cloud)
        changesshConfig(vm,cloud)
        print("##### Finish the deloying!")
    except Exception, e:
        logger.error("Error in __main__ method : " + str(e))
