#!/usr/bin/env python3

import subprocess
import argparse
import tempfile
import shutil
import os

from katsdpcontroller.generator import IMAGES


EXTRA_COPY = ['katsdpcontroller', 'docker-base-runtime', 'docker-base-build']
MANUAL = ['docker-base-gpu-build', 'docker-base-gpu-runtime',
          'katsdpingest', 'katsdpingest_titanx',
          'katcbfsim', 'katsdpcontim', 'katsdpimager',
          # Images below here should eventually exist on quay.io
          'katsdpcal', 'katsdpcontroller']
REPO_MAP = {
    'docker-base-gpu-build': 'katsdpdockerbase',
    'docker-base-gpu-runtime': 'katsdpdockerbase',
    'katsdpcal': 'katsdppipelines',
    'katsdpcontim': 'katsdppipelines'
}
# Branches to build instead of master. These should be temporary, for cases
# where repositories don't yet build outside of SARAO.
BRANCH_MAP = {
    'katsdpimager': 'more-packaging',
    'katsdpcontroller': 'image-builder'
}
SPECIAL = frozenset({'katsdpingest_titanx'})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--upstream', help='upstream registry from which to pull pre-built images',
                        default='quay.io/ska-sa')
    parser.add_argument('--downstream', help='registry to push images into',
                        default='localhost:5000')
    args = parser.parse_args()
    return args


def upstream_path(name: str, args: argparse.Namespace) -> str:
    return f'{args.upstream}/{name}:latest'


def downstream_path(name: str, args: argparse.Namespace) -> str:
    return f'{args.downstream}/{name}:latest'


def docker_cmd(*args: str) -> None:
    subprocess.run(['docker'] + list(args), stdin=subprocess.DEVNULL, check=True)


def build_image(name: str, args: argparse.Namespace) -> None:
    tmpdir = tempfile.mkdtemp()
    try:
        repo = REPO_MAP.get(name, name)
        url = f'https://github.com/ska-sa/{repo}'
        branch = BRANCH_MAP.get(name, 'master')
        print(f'Checking out {url} branch {branch} to build {repo}')
        # GIT_TERMINAL_PROMPT=0 prevents Git from asking for credentials,
        # which could happen if a typo meant the repository did not exist.
        subprocess.run(
            [
                'git', 'clone',
                '--branch', branch,
                '--recurse-submodules',
                '--depth=1', '--shallow-submodules',
                url, '.'
            ], stdin=subprocess.DEVNULL, check=True, cwd=tmpdir,
            env={**os.environ, 'GIT_TERMINAL_PROMPT': '0'})
        git_rev = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, check=True, cwd=tmpdir,
            encoding='utf-8').stdout.strip()

        downstream_image = downstream_path(name, args)
        print(f'Building image {downstream_image}')
        workdir = os.path.join(tmpdir, name) if name in REPO_MAP else tmpdir
        if os.path.exists(os.path.join(workdir, 'scripts', 'docker_build.sh')):
            docker_build = ['scripts/docker_build.sh']
        else:
            docker_build = ['docker', 'build']
        subprocess.run(
            docker_build + [
                '--label=org.label-schema.schema-version=1.0',
                f'--label=org.label-schema.vcs-ref={git_rev}',
                f'--label=org.label-schema.vcs-url={url}',
                '--build-arg', f'KATSDPDOCKERBASE_REGISTRY={args.downstream}',
                '--build-arg', f'KATSDPDOCKERBASE_MIRROR=',
                '--pull=true',
                '-t', downstream_image, '.'
            ], stdin=subprocess.DEVNULL, check=True, cwd=workdir)
        print(f'Pushing {downstream_image}')
        docker_cmd('push', downstream_image)
        print(f'{downstream_image} pushed')
    finally:
        shutil.rmtree(tmpdir)


def copy_image(name: str, args: argparse.Namespace) -> None:
    upstream_image = upstream_path(name, args)
    downstream_image = downstream_path(name, args)
    print(f'Pulling {upstream_image}')
    docker_cmd('pull', upstream_image)
    print(f'Tagging {upstream_image} -> {downstream_image}')
    docker_cmd('tag', upstream_image, downstream_image)
    print(f'Pushing {downstream_image}')
    docker_cmd('push', downstream_image)
    print(f'{downstream_image} pushed')


def main() -> None:
    args = parse_args()
    names = sorted(IMAGES) + EXTRA_COPY
    for name in names:
        if name not in MANUAL:
            copy_image(name, args)
    for name in MANUAL:
        if name not in SPECIAL:
            build_image(name, args)


if __name__ == '__main__':
    main()
