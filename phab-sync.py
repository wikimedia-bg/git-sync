#!/usr/bin/env python3

import json
import os
import os.path
import re
import signal
import sys
import time

from datetime import datetime as dt
from datetime import timedelta as td
from pathlib import Path

import git
import pywikibot as pwb


class SignalHandler:

    def __init__(self):
        self._is_sleeping = False
        self._exit_requested = False
        signal.signal(signal.SIGINT, self._request_exit)
        signal.signal(signal.SIGTERM, self._request_exit)

    def _request_exit(self, signal, frame):
        if self._is_sleeping:
            self._exit_now()
        else:
            self._exit_requested = True

    def _exit_now(self):
        print('SIGINT or SIGTERM received, exiting...')
        sys.exit(0)

    def sleep(self, seconds):
        if self._exit_requested:
            self._exit_now()
        else:
            self._is_sleeping = True
            time.sleep(seconds)
            self._is_sleeping = False


class PhabRepo:

    def __init__(self, repo, site, namespace, title_regex, force_ext, ignores):
        self.repo = repo
        self.site = site
        self.namespace = namespace
        self.title_regex = title_regex
        self.force_ext = force_ext
        self.ignores = ignores
        self._pending_files = []

    def _allpages(self):
        return self.site.allpages(namespace=self.namespace)

    def _last_changed(self):
        return dt.utcfromtimestamp(self.repo.commit('master').authored_date) + td(seconds=1)

    def _revlist(self):
        revs = []
        for page in self._allpages():
            page_name = page.title(with_ns=False)
            if self.title_regex.search(page_name):
                for rev in page.revisions(endtime=self._last_changed(), content=True):
                    revs.append((page_name, rev))
        revs.sort(key=lambda rev: rev[1]['timestamp'])
        return revs

    def _pull(self):
        old_master = self.repo.commit('master')
        self.repo.git.pull()
        # Obtain all changed files in this pull.
        self._pending_files += self.repo.git.diff_tree(
                '--no-commit-id',
                '--name-only',
                '-r',
                old_master,
                self.repo.commit('master')).split('\n')

    def _wiki2phab(self):
        revs = self._revlist()
        for rev in revs:
            if rev[1]['user'] == self.site.username():
                continue
            elif rev[1]['user'] == 'Iliev':
                user_mail = 'luchesar.iliev@gmail.com'
            else:
                user_mail = ''
            if not rev[1]['comment']:
                comment = '*** празно резюме ***'
            else:
                comment = rev[1]['comment']
            # We cannot have both a file and a directory with the same name, so where we have
            # 'Page' and 'Page/doc', the latter gets converted to 'Page.d/doc'.
            file_name = rev[0].replace('/', '.d/')
            # If we've configured a file extension for syntax highlighting, add it, but only for
            # files in the root of the namespace/repository (the rest will likely be 'Page/doc').
            if self.force_ext and '.d/' not in file_name:
                file_name = file_name + '.' + self.force_ext
            file_path = os.path.join(self.repo.working_dir, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # To avoid conflicts as much as possible, perform git pull right before we apply the
            # change and commit it.
            self._pull()
            with open(file_path, 'w') as f:
                f.write(rev[1]['text'])
            self.repo.index.add([file_path])
            author = git.Actor(rev[1]['user'].replace(' ', '_'), user_mail)
            committer = git.Actor(rev[1]['user'].replace(' ', '_'), user_mail)
            print('Syncing to Phabricator: {}'.format(file_name))
            self.repo.index.commit(
                    comment,
                    author=author,
                    committer=committer,
                    author_date=str(rev[1]['timestamp'])[:-1],
                    commit_date=str(rev[1]['timestamp'])[:-1])
            # Push after each commit. It's inefficient, but should minimize possible conflicts.
            self.repo.git.push()
        # Return the list of files that have been synced from Wikipedia.
        return [_[0].replace('/', '.d/') for _ in revs]

    def _phab2wiki(self, files_tosync):
        for file_name in files_tosync:
            # We cannot have both a file and a directory with the same name, so where we have
            # 'Page' and 'Page/doc', the latter was converted to 'Page.d/doc'.
            page_name = self.namespace + ':' + file_name.replace('.d/', '/')
            # If we've configured a file extension for syntax highlighting, remove it, but only for
            # files in the root of the namespace/repository (the rest will likely be 'Page/doc').
            if self.force_ext and '/' not in page_name:
                page_name = page_name.replace('.' + self.force_ext, '')
            page = pwb.Page(self.site, page_name)
            file_path = os.path.join(self.repo.working_dir, file_name)
            try:
                with open(file_path, 'r') as f:
                    page.text = f.read()
                print('Saving {}'.format(page.title()))
                page.save(summary='Committing changes from Phabricator', botflag=True, quiet=True)
            except FileNotFoundError:
                print('Deleting {}'.format(page.title()))
                page.delete(reason='Committing changes from Phabricator', prompt=False)
            except pwb.data.api.APIError as e:
                print('APIError exception: {}'.format(str(e)), file=sys.stderr)

    def sync(self):
        w2ph_synced_files = self._wiki2phab()
        self._pull()
        # If no files have been changed in Phabricator, self._pending_files is set to [''] (list
        # with a single element, which itself is an empty string). This is _not_ an empty list.
        ph2w_pending_files = list(filter(None, self._pending_files))
        # Determine which pages need to be updated on Wikipedia. To be on the safe side, if during
        # one sync operation a page is changed both on Wikipedia and on Phabricator, the Wikipedia
        # version wins. We do this by simply substracting the list of files that were updated on
        # Wikipedia from the list of files updated on Phabricator (converting to a set() first).
        # We also remove the list of files to ignore, e.g. .arcconfig, .arclint, etc.
        ph2w_files_tosync = set(ph2w_pending_files) - set(w2ph_synced_files) - set(self.ignores)
        if ph2w_files_tosync:
            print('Syncing to Wikipedia: {}'.format(ph2w_files_tosync))
            self._phab2wiki(ph2w_files_tosync)
            self._pending_files = []


def init_repos(config):
    repos = []
    for repo in config['repos']:
        file_regex = re.compile(
                repo['file_regex'],
                re.I if repo['regex_case'] else 0)
        repo_path = os.path.join(config['repositories_root'], repo['name'])
        git_repo = git.Repo(repo_path)
        site = pwb.Site(
                code=repo['project']['code'],
                fam=repo['project']['family'],
                user=config['mediawiki_username'],
                sysop=config['mediawiki_username'])
        repos.append(PhabRepo(git_repo, site, repo['namespace'],
                              file_regex, repo['force_extension'],
                              repo['ignore_list'] + config['global_ignore_list']))
    return repos


def read_config():
    config_file_name = 'phab-sync.config.json'
    config_files = [
            config_file_name,
            os.path.join(Path.home(), '.config/phab-sync', config_file_name),
            os.path.join('/etc/phab-sync', config_file_name),
            ]
    for config_file in config_files:
        if os.path.exists(config_file):
            with open(config_file, 'rb') as f:
                config = json.load(f)
            break
    try:
        return(config)
    except UnboundLocalError:
        print('Error: Configuration file not found.', file=sys.stderr)
        sys.exit(1)


def main(argv):
    sig_handler = SignalHandler()
    config = read_config()
    repos = init_repos(config)
    while True:
        for repo in repos:
            print('Syncing repo "{}"...'.format(repo.repo.git_dir))
            repo.sync()
        print('Sleeping...')
        sig_handler.sleep(config['daemon_sleep_seconds'])


if __name__ == '__main__':
    main(sys.argv[1:])

# vim: set ts=4 sts=4 sw=4 et:
