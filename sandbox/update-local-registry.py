#!/usr/bin/env python3

import enum
import subprocess
import argparse
import tempfile
import shutil
import os
import logging
from typing import List, Set, Iterable, Optional

import katsdpcontroller.generator


class Action(enum.Enum):
    COPY = 1
    BUILD = 2
    TUNE = 3


class ImageInfo:
    def __init__(self, *,
                 action: Action = Action.COPY,
                 repo: Optional[str] = None,
                 branch: str = 'master'):
        self.action = action
        self.repo = repo
        self.branch = branch


# Order is important here: images are built in order
EXTRA_IMAGES = ['docker-base-runtime', 'docker-base-build',
                'docker-base-gpu-build', 'docker-base-gpu-runtime',
                'katsdpingest']
IMAGES = EXTRA_IMAGES + list(katsdpcontroller.generator.IMAGES)

IMAGE_INFO = {
    'docker-base-runtime': ImageInfo(repo='katsdpdockerbase'),
    'docker-base-build': ImageInfo(repo='katsdpdockerbase'),
    'docker-base-gpu-build': ImageInfo(action=Action.BUILD, repo='katsdpdockerbase'),
    'docker-base-gpu-runtime': ImageInfo(action=Action.BUILD, repo='katsdpdockerbase'),
    'katsdpcal': ImageInfo(action=Action.BUILD, repo='katsdppipelines'),
    'katsdpcontim': ImageInfo(action=Action.BUILD, repo='katsdppipelines'),
    'katsdpingest_titanx': ImageInfo(action=Action.TUNE, repo='katsdpingest'),
    'katsdpingest': ImageInfo(action=Action.BUILD),
    'katsdpimager': ImageInfo(action=Action.BUILD, branch='more-packaging'),
    'katcbfsim': ImageInfo(action=Action.BUILD),
    # Will go away after merge
    'katsdpcontroller': ImageInfo(action=Action.BUILD, branch='image-builder')
}


def image_info(name: str) -> ImageInfo:
    return IMAGE_INFO.get(name, ImageInfo())


def expand_special(names: Iterable[str]) -> Set[str]:
    out: Set[str] = set()
    for name in names:
        if name == 'all':
            out |= set(IMAGES)
        elif name == 'copy':
            out |= set(image for image in IMAGES if image_info(image).action == Action.COPY)
        elif name == 'build':
            out |= set(image for image in IMAGES if image_info(image).action == Action.BUILD)
        elif name == 'tune':
            out |= set(image for image in IMAGES if image_info(image).action == Action.TUNE)
        elif name == 'none':
            pass
        else:
            if name not in IMAGES:
                logging.warning('Image %s not known', name)
            out.add(name)
    return out


def comma_split(x: str) -> List[str]:
    if not x:
        return []
    else:
        return x.split(',')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--upstream', default='quay.io/ska-sa',
                        help='upstream registry from which to pull pre-built images [%(default)s]')
    parser.add_argument('--downstream', default='localhost:5000',
                        help='registry to push images into [%(default)s]')
    parser.add_argument('--build-all', action='store_true',
                        help='Build all images rather than copying from upstream')
    parser.add_argument('--include', type=comma_split, default=['all'],
                        help='comma-separated list of images to update [all]')
    parser.add_argument('--exclude', type=comma_split, default=[],
                        help='comma-separated list of images to skip [none]')
    args = parser.parse_args()
    args.include = expand_special(args.include)
    args.exclude = expand_special(args.exclude)
    return args


def upstream_path(name: str, args: argparse.Namespace) -> str:
    return f'{args.upstream}/{name}:latest'


def downstream_path(name: str, args: argparse.Namespace) -> str:
    return f'{args.downstream}/{name}:latest'


def docker_cmd(*args: str) -> None:
    subprocess.run(['docker'] + list(args), stdin=subprocess.DEVNULL, check=True)


def build_image(name: str, args: argparse.Namespace) -> None:
    info = image_info(name)
    tmpdir = tempfile.mkdtemp()
    try:
        repo = info.repo or name
        url = f'https://github.com/ska-sa/{repo}'
        branch = info.branch
        logging.info('Checking out %s branch %s to build %s', url, branch, repo)
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
        logging.info('Building image %s', downstream_image)
        workdir = os.path.join(tmpdir, name) if info.repo else tmpdir
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
        logging.info('Pushing %s', downstream_image)
        docker_cmd('push', downstream_image)
        logging.info('%s pushed', downstream_image)
    finally:
        shutil.rmtree(tmpdir)


def copy_image(name: str, args: argparse.Namespace) -> None:
    upstream_image = upstream_path(name, args)
    downstream_image = downstream_path(name, args)
    logging.info('Pulling %s', upstream_image)
    docker_cmd('pull', upstream_image)
    logging.info('Tagging %s -> %s', upstream_image, downstream_image)
    docker_cmd('tag', upstream_image, downstream_image)
    logging.info('Pushing %s', downstream_image)
    docker_cmd('push', downstream_image)
    logging.info('%s pushed', downstream_image)


def main() -> None:
    logging.basicConfig(level='INFO')
    args = parse_args()

    for name in IMAGES:
        if name not in args.include or name in args.exclude:
            continue
        info = image_info(name)
        if info.action == Action.TUNE:
            logging.warning('Autotuning not implemented yet (%s)', name)
        elif info.action == Action.BUILD or args.build_all:
            build_image(name, args)
        else:
            copy_image(name, args)


if __name__ == '__main__':
    main()
