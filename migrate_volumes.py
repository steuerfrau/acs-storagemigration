#!/usr/bin/python3

"""Tools to automate volume migration in ACS."""
import sys
import time
import glob
import os
# import pprint
import argparse
import textwrap
from cs import CloudStack, read_config

PARSER = argparse.ArgumentParser(
    prog='migrate_volumes.py',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent('''\
    Migrates all volumes from source storage to destination storage. One
    after the other.

    Autor: Melanie Desaive <m.desaive@heinlein-support.de>
    '''),
    epilog=textwrap.dedent('''\
    Examples:

    Prepare list with volumes from project and one LUN for migration:
        ./migrate_volumes.py --prepare-migratelist
            --prep-proj MelTest --prep-sr ACS-LUN003-SATA
            --output-list /tmp/melvolumes-lun003.txt

    Migrate all volumes according to list
        ./migrate_volumes.py --do-migrate \
                --input-list /tmp/somevolumes.csv \
                --dest-storage SAN1-XEN01-0017

    Monitor Jobstatus
       watch python3 migrate_volumes.py --monitor-migrations

    Additional Infos:

    Uses the "CS" CloudStack API Client. See https://github.com/exoscale/cs.
    To install use "pip install cs".

    Requires configuration file ~/.cloudstack.ini.

    '''))

PARSER.add_argument('--prepare-migratelist',
                    dest='prepare_migratelist',
                    action='store_true',
                    help='Prepare list with volumes.',
                    required=False)
PARSER.add_argument('--prep-sr',
                    dest='prep_sr',
                    help='Storage Repo with volumes for prepare list.',
                    required=False)
PARSER.add_argument('--prep-proj',
                    dest='prep_proj',
                    help='Pick volumes from this project for prepare list.',
                    required=False)
PARSER.add_argument('--output-list',
                    dest='output_list',
                    help='Write migratelist to file.',
                    required=False)


PARSER.add_argument('--interactive',
                    dest='interactive',
                    action='store_true',
                    help='Wait for confirmation before each volume.',
                    required=False)
PARSER.add_argument('--non-interactive',
                    dest='non_interactive',
                    action='store_true',
                    help='Don\'t for confirmation before each volume.',
                    required=False)
PARSER.add_argument('--do-migrate',
                    dest='do_migrate',
                    action='store_true',
                    help='Start to migrate volumes.',
                    required=False)
PARSER.add_argument('--input-list',
                    dest='input_list',
                    help='Read migratelist from file.',
                    required=False)
PARSER.add_argument('--dest-storage',
                    dest='dest_storage',
                    help='Destination storage.',
                    required=False)


PARSER.add_argument('--monitor-migrations',
                    dest='monitor_migrations',
                    help='Monitor migration progress.',
                    action='store_true',
                    required=False)

ARGS = PARSER.parse_args()


def get_project_id(projectname):
    """Return project_id for projectname!"""
    project_container = CS.listProjects(listall=True)
    projects = project_container["project"]

    for project in projects:
        if project["name"] == projectname:
            return project["id"]

    print('Valid project names are:')
    for project in sorted(projects, key=lambda key: (key["name"])):
        print('{name}'.format(name=project["name"]))
    raise NameError('Projectname unknown.')


def collect_volumes(project_id, overall_volumes):
    """Collect information about all volumes through ACS API!"""
    # print(f'List volumes for project_id=\"{project_id}\".')
    if project_id != 'n.a.':
        volumes_container = CS.listVolumes(
            listall=True, projectid=project_id)
    else:
        project_id = 'n.a.'
        volumes_container = CS.listVolumes(listall=True)

    # print(f'{volumes_container}')
    if volumes_container:
        volumes = volumes_container["volume"]
    else:
        volumes = {}

    # pprint.pprint(volumes)
    for volume in volumes:
        if "vmname" not in volume:
            volume.update({'vmname': 'n.a.'})
        if "vmstate" not in volume:
            volume.update({'vmstate': 'n.a.'})
        if "project" not in volume:
            volume.update({'project': 'n.a.'})

        overall_volumes.append(volume)
    # pprint.pprint(overall_volumes)


def printout_volumes(output_list, overall_volumes):
    """Print list of all volumes in overall_volumes!"""
    output_list.write(
        'id;domain;project;vmname;vmstate;name;state;storage;size\n')

    for volume in sorted(overall_volumes, key=lambda i: (
            i['domain'].lower(), i['project'].lower(),
            i['vmname'].lower(), i['name'].lower())):
        volume_id = volume["id"]
        volume_name = volume["name"]
        if "vmname" not in volume:
            volume_vmname = 'n.a.'
        else:
            volume_vmname = volume["vmname"]
        if "vmstate" not in volume:
            volume_vmstate = 'n.a.'
        else:
            volume_vmstate = volume["vmstate"]
        volume_domain = volume["domain"]
        if "project" not in volume:
            volume_project = 'n.a.'
        else:
            volume_project = volume["project"]
        if "storage" in volume:
            volume_storage = volume["storage"]
        else:
            volume_storage = 'n.a.'
        volume_size = volume["size"]
        volume_state = volume["state"]
        # if "status" not in volume:
        #     volume_status = 'n.a.'
        # else:
        #     volume_status = volume["status"]

        if ARGS.prep_sr and ARGS.prep_sr != volume_storage:
            continue

        output_list.write(
            f'{volume_id};{volume_domain};{volume_project};'
            f'{volume_vmname};{volume_vmstate};{volume_name};'
            f'{volume_state};{volume_storage};{volume_size}\n')


def prepare_output_list():
    """Iterate over all projects and build list of all volumes!"""
    if ARGS.output_list is not None:
        output_list = open(ARGS.output_list, 'w')
    else:
        output_list = sys.stdout

    projects_container = CS.listProjects(listall=True)
    projects = projects_container["project"]

    overall_volumes = []
    if ARGS.prep_proj:
        project_id = get_project_id(ARGS.prep_proj)

        collect_volumes(project_id, overall_volumes)
    else:
        for project in sorted(projects, key=lambda key: key["name"]):
            # project_name = project["name"]
            # project_id = project["id"]
            # print(f'{project_id}')
            collect_volumes(project["id"], overall_volumes)
        collect_volumes('n.a.', overall_volumes)

    # pprint.pprint(overall_volumes)
    printout_volumes(output_list, overall_volumes)


def do_migrate(prefix, dst_storage):
    """Migrate volumes defined by textfile!"""
    with open(ARGS.input_list) as input_list:
        for line in input_list:
            fields = line.split(";")
            fields_dict = {}
            fields_dict.update({'id': fields[0]})
            fields_dict.update({'domain': fields[1]})
            fields_dict.update({'project': fields[2]})
            fields_dict.update({'vmname': fields[3]})
            fields_dict.update({'vmstate': fields[4]})
            fields_dict.update({'name': fields[5]})
            fields_dict.update({'state': fields[6]})
            fields_dict.update({'storage': fields[7]})
            fields_dict.update({'size': fields[8]})

            if fields_dict['id'] == 'id':
                continue

            volume_id = fields_dict['id']
            volumes_container = CS.listVolumes(listall=True, id=volume_id)
            volumes = volumes_container["volume"]

            # volume_vmstate = volumes[0]['vmstate']
            # volume_state = volumes[0]['state']
            # volume_storage = volumes[0]['storage']
            # volume_name = volumes[0]['name']
            # volume_vmname = volumes[0]['vmname']

            if volumes[0]['vmstate'] == 'Running':
                migrate_mode = 'live'
            elif volumes[0]['vmstate'] == 'Stopped':
                migrate_mode = 'offline'
            else:
                raise Exception('Unexpected VMState!')

            print('----------------------------------')
            print(f'Please confirm migration ({migrate_mode:7}): '
                  f'{volumes[0]["vmname"]:25} {volumes[0]["name"]:17} '
                  f'{float(fields_dict["size"])/1024/1024/1024:20} GB         '
                  f'from {volumes[0]["storage"]:20} to '
                  f'{ARGS.dest_storage:20}       '
                  f'VM is {volumes[0]["vmstate"]} volume '
                  f'is {volumes[0]["state"]}')
            answer = None
            while answer not in ("yes", "no"):
                answer = input("Enter yes or no: ")
                if answer == "yes":
                    print('Yes! Migrating....')
                    if migrate_mode == 'live':
                        migrate_answer = CS.migrateVolume(
                            volumeid=fields_dict['id'],
                            storageid=dst_storage,
                            livemigrate=True)
                    if migrate_mode == 'offline':
                        migrate_answer = CS.migrateVolume(
                            volumeid=fields_dict['id'],
                            storageid=dst_storage)
                    jobid = migrate_answer['jobid']

                    filename = '{prefix}{time}'.format(
                        prefix=prefix,
                        time=time.strftime("%Y%m%d-%H%M%S", time.gmtime()))
                    print(filename)
                    output_file = open(filename, 'w')
                    output_file.write(
                        f'{fields_dict["id"]};{jobid};'
                        f'{time.strftime("%Y%m%d-%H%M%S", time.gmtime())}')
                    output_file.close()

                elif answer == "no":
                    print(
                        f'Skipping {fields_dict["vmname"]}'
                        f'-{fields_dict["name"]}')
                else:
                    print("Please enter yes or no.")


# def writeout_joblist(joblist, prefix):
#     filename = f'{prefix}{time.strftime("%Y%m%d-%H%M%S", time.gmtime())}'
#     print(filename)
#     output_list = open(filename, 'w')
#     for job in joblist:
#         output_list.write(
#             f'{job["volumeid"]};{job["jobid"]};{job["started"]}\n')
#     output_list.close()


def migration_status(prefix):
    """Print status of last migrations!"""
    status_list = []
    os.chdir(os.path.dirname(prefix))
    for file in glob.glob(f'{os.path.basename(prefix)}*'):
        with open(file) as input_file:
            lines = input_file.readlines()
            for line in lines:
                fields = line.split(';')
                volumeid = fields[0]
                jobid = fields[1]
                started = fields[2].rstrip()

                volumes_container = CS.listVolumes(
                    listall=True, id=volumeid)
                # pprint.pprint(volumes_container)
                volumes = volumes_container['volume']

                # for volume in volumes:
                #     volume_name = volume['name']
                #     volume_vmname = volume['vmname']
                #     volume_storage = volume['storage']

                jobs_container = CS.queryAsyncJobResult(
                    listall=True, jobid=jobid)
                # pprint.pprint(jobs_container)

                status_list.append(
                    {
                        'vmname': volumes[0]["vmname"],
                        'vmstate': volumes[0]["vmstate"],
                        'name': volumes[0]["name"], 'size': volumes[0]["size"],
                        'volume_state': volumes[0]["state"],
                        'storage': volumes[0]["storage"],
                        'volumeid': volumeid, 'started': started,
                        'job-status': jobs_container["jobstatus"],
                        'job-resultcode': jobs_container["jobresultcode"]})

    print('Started         Status VM-Name' +
          '                   VM-State Volume-Name       '
          '           Size State      '
          'Storage              Volume-ID                             '
          'Job-Resultcode')
    for job in sorted(
            status_list, key=lambda i: (i['job-status'], i['started'])):
        print(
            f'{job["started"]} {job["job-status"]:6} '
            f'{job["vmname"]:25} {job["vmstate"]:8} '
            f'{job["name"]:25} {job["size"]/1024/1024/1024:7} '
            f'{job["volume_state"]:10} '
            f'{job["storage"]:20} '
            f'{job["volumeid"]}; '
            f'{job["job-resultcode"]}')


def get_storageid(acs, storage_name):
    """Get storageid from name."""
    storages_container = acs.listStoragePools(listall=True)
    storages = storages_container["storagepool"]
    # pprint.pprint(storages)
    storage_id = ''
    for storage in storages:
        if storage["name"] == storage_name:
            storage_id = storage["id"]
    return storage_id


# Reads ~/.cloudstack.ini
CS = CloudStack(**read_config())

# Check ARGS.dest_storage
if ARGS.dest_storage:
    if get_storageid(CS, ARGS.dest_storage) == '':
        raise Exception(
            'The storage \"{storage}\" does not exist'.format(
                storage=ARGS.dest_storage))

if ARGS.prep_sr:
    if get_storageid(CS, ARGS.prep_sr) == '':
        raise Exception(
            'The storage \"{storage}\" does not exist'.format(
                storage=ARGS.prep_sr))

if ARGS.prepare_migratelist and ARGS.do_migrate:
    raise Exception(
        '--prepare_migrate and --do-migrate cannot be used together.')

if ARGS.interactive and ARGS.non_interactive:
    raise Exception(
        '--interactive and --non-interactive cannot be used together.')

if ARGS.do_migrate and not ARGS.input_list:
    raise Exception('Please enter --input-list with --do-migrate.')

if ARGS.do_migrate and not ARGS.dest_storage:
    raise Exception('Please enter --dest-storage with --do-migrate.')

if ARGS.prepare_migratelist:
    prepare_output_list()

if ARGS.monitor_migrations:
    migration_status('/tmp/joblist-')

if ARGS.do_migrate:
    DST_STORAGEID = get_storageid(CS, ARGS.dest_storage)
    if DST_STORAGEID == '':
        sys.exit(1)
    # joblist = []
    print('Migrate to storage_id \"' + DST_STORAGEID + '\"')
    do_migrate('/tmp/joblist-', DST_STORAGEID)
    # print(f'Joblist:\n{joblist}')
    # writeout_joblist(joblist, '/tmp/joblist-')
