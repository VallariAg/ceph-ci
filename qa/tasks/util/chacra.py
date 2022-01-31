#!/usr/bin/env python3

import argparse
import requests
import sys

from pathlib import Path
from urllib.parse import urlparse


PROJECT = 'ceph'
DISTRO = 'ubuntu'
RELEASE = 'focal'
ARCH='x86_64'
BRANCH = 'master'
SHA1 = 'latest'
FLAVOR = 'default'
FILENAME = 'cephadm'


def search(*args, **kwargs):
    '''
    Query shaman for a build result
    '''
    endpoint = 'https://shaman.ceph.com/api/search'
    resp = requests.get(endpoint, params=kwargs)
    resp.raise_for_status()
    return resp

def get_binary(host, project, ref, sha1,
               distro, release, arch, flavor,
               filename):
    '''
    Pull a binary from chacra
    '''
    base_url = f'https://{host}/binaries'
    bin_path = f'{project}/{ref}/{sha1}/{distro}/{release}/{arch}/flavors/{flavor}'
    bin_file = f'{base_url}/{bin_path}/{filename}'
    resp = requests.get(bin_file, stream=True)
    resp.raise_for_status()
    return resp

def pull(
    src, dest,
    project=None,
    distro=None,
    release=None,
    arch=None,
    flavor=None,
    branch=None,
    sha1=None):

    def _get_distros(distro, release, arch=None):
        ret = f'{distro}/{release}'
        if arch:
            ret = f'{ret}/{arch}'
        return ret

    # query shaman for the built binary
    s = {}
    if project:
        s['project'] = project
    if distro:
        s['distros'] = _get_distros(distro, release, arch)
    if flavor:
        s['flavor'] = flavor
    if branch:
        s['ref'] = branch
    if sha1:
        s['sha1'] = sha1
    resp = search(**s)

    resp_json = resp.json()
    if len(resp_json) == 0:
        raise RuntimeError(f'no results found at {resp.url}')

    # check the build status
    config = resp_json[0]
    status = config['status']
    if status != 'ready':
        raise RuntimeError(f'cannot pull file with status: {status}')
    # TODO: assert len() == 1?

    # pull the image from chacra
    chacra_host = urlparse(config['url']).netloc
    chacra_ref = config['ref']
    chacra_sha1 = config['sha1']
    print('got chacra host {}, ref {}, sha1 {} from {}'.format(
        chacra_host, chacra_ref, chacra_sha1, resp.url))

    resp = get_binary(
            chacra_host,
            config['project'],
            chacra_ref,
            chacra_sha1,
            config['distro'],
            config['distro_codename'] if config['distro_codename'] else config['distro_version'],
            config['archs'][0],
            config['flavor'],
            src,
    )
    print(f'got file from {resp.url}')

    dest = Path(dest).absolute()
    with open(dest, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=None, decode_unicode=True):
            print('.',)
            f.write(chunk)
    print(f'wrote binary file: {dest}')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default=PROJECT)
    parser.add_argument('--distro', default=DISTRO)
    parser.add_argument('--release', default=RELEASE)
    parser.add_argument('--arch', default=ARCH)
    parser.add_argument('--branch', default=BRANCH)
    parser.add_argument('--sha1', default=SHA1)
    parser.add_argument('--flavor', default=FLAVOR)
    parser.add_argument('--src', default=FILENAME)
    parser.add_argument('--dest', default=FILENAME)
    args = parser.parse_args()

    pull(
        args.src, args.dest,
        project=args.project,
        distro=args.distro,
        release=args.release,
        arch=args.arch,
        flavor=args.flavor,
        branch=args.branch,
        sha1=args.sha1
    )

    return 0


if __name__ == '__main__':
    sys.exit(main())